package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MACOS-DO/sub4api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Use the production dialer over a real socket, changing only the destination
// so OAuth requests never contact a provider during this regression.
type codexWireSocketDialer struct {
	url   string
	inner openAIWSClientDialer
}

func (d codexWireSocketDialer) Dial(ctx context.Context, _ string, headers http.Header, _ string) (openAIWSClientConn, int, http.Header, error) {
	return d.inner.Dial(ctx, d.url, headers, "")
}

func TestCodexWSCompatibilityMetadataOnSocket(t *testing.T) {
	gin.SetMode(gin.TestMode)
	type observed struct {
		headers http.Header
		body    []byte
		err     error
	}
	requests := make(chan observed, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			requests <- observed{err: err}
			return
		}
		conn.SetReadLimit(2 << 20)
		for {
			_, body, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			requests <- observed{headers: r.Header.Clone(), body: body}
			if err := conn.Write(r.Context(), coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_wire","status":"completed","output":[],"usage":{"input_tokens":7,"output_tokens":1}}}`)); err != nil {
				return
			}
		}
	}))
	defer upstream.Close()
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(codexWireSocketDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http"), inner: newDefaultOpenAIWSClientDialer()})
	defer pool.Close()
	svc := &OpenAIGatewayService{cfg: cfg, openaiWSPool: pool, toolCorrector: NewCodexToolCorrector()}
	account := &Account{ID: 571, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1, Credentials: map[string]any{"chatgpt_account_id": "wire-test-account"}}
	metadata := `{"session_id":"wire-session","thread_id":"wire-thread","request_kind":"turn","tool_namespaces_info":{"tools":{"name":"tools","functions":{"execute":{"name":"execute","direct":true}}}}}`
	inventoryName := strings.Repeat("x", 70<<10)
	metadata = strings.Replace(metadata, `"name":"tools"`, `"name":"`+inventoryName+`"`, 1)
	for range 2 {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		c.Request.Header.Set("session-id", "wire-session")
		c.Request.Header.Set("thread-id", "wire-thread")
		c.Request.Header.Set("x-responsesapi-include-timing-metrics", "true")
		c.Request.Header.Set("x-openai-subagent", "review")
		c.Request.Header.Set("x-openai-memgen-request", "true")
		c.Request.Header.Set("x-oai-attestation", "must-not-forward-client-proof")
		payload := map[string]any{"model": "gpt-5.6-sol", "input": "hello", "client_metadata": map[string]any{"x-codex-turn-metadata": metadata}}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result, err := svc.forwardOpenAIWSV2(ctx, c, account, payload, "wire-session", "wire-scope", openAIWSSessionIdentity{}, "test-token", OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2}, true, true, "gpt-5.6-sol", "gpt-5.6-sol", time.Now(), 1, "", new(bool))
		cancel()
		require.NoError(t, err)
		require.Equal(t, "response.completed", result.UpstreamTerminalEvent)
		got := <-requests
		require.NoError(t, got.err)
		require.Equal(t, "true", got.headers.Get("x-responsesapi-include-timing-metrics"))
		require.Equal(t, "review", got.headers.Get("x-openai-subagent"))
		require.Equal(t, "true", got.headers.Get("x-openai-memgen-request"))
		for _, name := range []string{"session_id", "conversation_id", "x-codex-installation-id", "x-oai-attestation"} {
			require.Empty(t, got.headers.Get(name), name)
		}
		headerMetadata := gjson.Parse(got.headers.Get(openAIWSTurnMetadataHeader))
		require.False(t, headerMetadata.Get("tool_namespaces_info").Exists(), "unbounded tool inventory must not be in the HTTP upgrade header")
		bodyMetadata := gjson.Parse(gjson.GetBytes(got.body, "client_metadata.x-codex-turn-metadata").String())
		require.Equal(t, "execute", bodyMetadata.Get("tool_namespaces_info.tools.functions.execute.name").String())
		require.Equal(t, inventoryName, bodyMetadata.Get("tool_namespaces_info.tools.name").String())
		require.Equal(t, bodyMetadata.Get("thread_id").String(), got.headers.Get("thread-id"))
		require.Equal(t, "response.create", gjson.GetBytes(got.body, "type").String())
		require.Contains(t, rec.Body.String(), "response.completed")
	}
}
