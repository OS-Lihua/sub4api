package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MACOS-DO/sub4api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func newAffinityWireService(t *testing.T, rejectDelta bool, outputs ...string) (*OpenAIGatewayService, *Account, <-chan []byte) {
	t.Helper()
	requests := make(chan []byte, 16)
	var sequence atomic.Int64
	var rejected atomic.Bool
	output := affinityOutput
	if len(outputs) > 0 {
		output = outputs[0]
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, payload, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			requests <- payload
			var response []byte
			if rejectDelta && gjson.GetBytes(payload, "previous_response_id").String() != "" && rejected.CompareAndSwap(false, true) {
				response = []byte(`{"type":"error","error":{"code":"previous_response_not_found","message":"context lost"}}`)
			} else {
				id := "resp_affinity_" + strconv.FormatInt(sequence.Add(1), 10)
				response = []byte(`{"type":"response.completed","response":{"id":"` + id + `","model":"gpt-5.1","status":"completed","output":[` + output + `],"usage":{"input_tokens":2,"output_tokens":1}}}`)
			}
			if err := conn.Write(r.Context(), coderws.MessageText, response); err != nil {
				return
			}
		}
	}))
	t.Cleanup(upstream.Close)
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 4
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 2
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 2
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 2
	svc := &OpenAIGatewayService{cfg: cfg, cache: &stubGatewayCache{}, toolCorrector: NewCodexToolCorrector(), openaiWSResolver: NewOpenAIWSProtocolResolver(cfg)}
	t.Cleanup(svc.getOpenAIWSConnPool().Close)
	account := &Account{ID: 90, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1,
		Credentials: map[string]any{"api_key": "test-key", "base_url": upstream.URL},
		Extra:       map[string]any{"responses_websockets_v2_enabled": true}}
	return svc, account, requests
}

func TestOpenAIWSSessionAffinity_ToolOutputContextLossReplaysCompleteHistory(t *testing.T) {
	toolCall := `{"type":"function_call","call_id":"call_echo","name":"echo","arguments":"{}"}`
	upstreamToolCall := `{"type":"function_call","id":"fc_echo","status":"completed","call_id":"call_echo","name":"echo","arguments":"{}"}`
	svc, account, requests := newAffinityWireService(t, true, upstreamToolCall)
	_, err := svc.Forward(context.Background(), affinityTestContext(1), account, []byte(affinityFirstPayload))
	require.NoError(t, err)
	<-requests
	nextPayload := []byte(`{"type":"response.create","model":"gpt-5.1","instructions":"system","input":[{"role":"user","content":"first"},` + toolCall + `,{"type":"function_call_output","call_id":"call_echo","output":"done"}]}`)
	result, err := svc.Forward(context.Background(), affinityTestContext(1), account, nextPayload)
	require.NoError(t, err)
	require.Equal(t, "response.completed", result.UpstreamTerminalEvent)
	delta := <-requests
	require.Equal(t, "resp_affinity_1", gjson.GetBytes(delta, "previous_response_id").String())
	require.Len(t, gjson.GetBytes(delta, "input").Array(), 1)
	require.Equal(t, "function_call_output", gjson.GetBytes(delta, "input.0.type").String())
	replay := <-requests
	require.Empty(t, gjson.GetBytes(replay, "previous_response_id").String())
	require.Len(t, gjson.GetBytes(replay, "input").Array(), 3)
	require.Equal(t, "call_echo", gjson.GetBytes(replay, "input.1.call_id").String())
	require.Equal(t, "call_echo", gjson.GetBytes(replay, "input.2.call_id").String())
}

func TestOpenAIWSSessionAffinity_HTTPWireAndLostContextReplay(t *testing.T) {
	for _, reject := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			t.Run(map[bool]string{false: "incremental", true: "context_lost"}[reject]+"/stream_"+strconv.FormatBool(stream), func(t *testing.T) {
				svc, account, requests := newAffinityWireService(t, reject)
				for _, payload := range []string{affinityFirstPayload, affinityNextPayload} {
					var request map[string]any
					require.NoError(t, json.Unmarshal([]byte(payload), &request))
					request["stream"] = stream
					body, err := json.Marshal(request)
					require.NoError(t, err)
					c := affinityTestContext(1)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					result, err := svc.Forward(ctx, c, account, body)
					cancel()
					require.NoError(t, err)
					require.Equal(t, "response.completed", result.UpstreamTerminalEvent)
				}
				first := <-requests
				require.Empty(t, gjson.GetBytes(first, "previous_response_id").String())
				next := <-requests
				require.Equal(t, "resp_affinity_1", gjson.GetBytes(next, "previous_response_id").String())
				require.Len(t, gjson.GetBytes(next, "input").Array(), 1)
				require.Equal(t, "next", gjson.GetBytes(next, "input.0.content").String())
				if reject {
					replay := <-requests
					require.Empty(t, gjson.GetBytes(replay, "previous_response_id").String())
					require.Len(t, gjson.GetBytes(replay, "input").Array(), 3, "reconnect must restore the complete original history")
				}
			})
		}
	}
}

func TestOpenAIWSSessionAffinity_HTTPExpiredAndOtherCallerSendFullHistory(t *testing.T) {
	for _, scenario := range []string{"expired", "other_key", "closed_socket"} {
		t.Run(scenario, func(t *testing.T) {
			svc, account, requests := newAffinityWireService(t, false)
			c := affinityTestContext(1)
			_, err := svc.Forward(context.Background(), c, account, []byte(affinityFirstPayload))
			require.NoError(t, err)
			<-requests
			pool := svc.getOpenAIWSConnPool()
			switch scenario {
			case "expired":
				pool.sessionAffinities.cleanup(time.Now().Add(openAIWSSessionAffinityTTL))
			case "other_key":
				c = affinityTestContext(2)
			case "closed_socket":
				binding := pool.sessionAffinities.lookup(openAIWSSessionIdentityForRequest(c, []byte(affinityFirstPayload)), "")
				require.NotNil(t, binding)
				pool.evictConn(account.ID, binding.conn.id)
			}
			_, err = svc.Forward(context.Background(), c, account, []byte(affinityNextPayload))
			require.NoError(t, err)
			next := <-requests
			require.Empty(t, gjson.GetBytes(next, "previous_response_id").String())
			require.Len(t, gjson.GetBytes(next, "input").Array(), 3)
		})
	}
}

func TestOpenAIWSSessionAffinity_NativeWSWireAndReconnect(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "incremental", true: "context_lost"}[reject], func(t *testing.T) {
			svc, account, requests := newAffinityWireService(t, reject)
			serverErrors := make(chan error, 2)
			ingress := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					serverErrors <- err
					return
				}
				defer conn.CloseNow()
				c := affinityTestContext(1)
				c.Request = r
				_, first, err := conn.Read(r.Context())
				if err == nil {
					err = svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "test-key", first, nil)
				}
				serverErrors <- err
			}))
			t.Cleanup(ingress.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			connect := func() *coderws.Conn {
				conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(ingress.URL, "http"), nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = conn.CloseNow() })
				return conn
			}
			send := func(conn *coderws.Conn, payload string) {
				require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(payload)))
				_, event, err := conn.Read(ctx)
				require.NoError(t, err)
				require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String())
			}
			conn := connect()
			send(conn, affinityFirstPayload)
			<-requests
			send(conn, affinityNextPayload)
			next := <-requests
			require.Equal(t, "resp_affinity_1", gjson.GetBytes(next, "previous_response_id").String())
			require.Len(t, gjson.GetBytes(next, "input").Array(), 1)
			if reject {
				replay := <-requests
				require.Empty(t, gjson.GetBytes(replay, "previous_response_id").String())
				require.Len(t, gjson.GetBytes(replay, "input").Array(), 3)
			}
			require.NoError(t, conn.Close(coderws.StatusNormalClosure, "reconnect"))
			require.NoError(t, <-serverErrors)

			conn = connect()
			third := `{"type":"response.create","model":"gpt-5.1","instructions":"system","input":[{"role":"user","content":"first"},{"role":"assistant","content":"answer"},{"role":"user","content":"next"},{"role":"assistant","content":"answer"},{"role":"user","content":"third"}]}`
			send(conn, third)
			actual := <-requests
			require.Equal(t, "resp_affinity_2", gjson.GetBytes(actual, "previous_response_id").String())
			require.Len(t, gjson.GetBytes(actual, "input").Array(), 1)
			var input []map[string]any
			require.NoError(t, json.Unmarshal([]byte(gjson.GetBytes(actual, "input").Raw), &input))
			require.Equal(t, "third", input[0]["content"])
			require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
			require.NoError(t, <-serverErrors)
		})
	}
}
