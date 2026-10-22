package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Replay interleaved windows with identical conversation heads. Request and turn
// IDs change each time, whereas thread identity survives transport reconnects.
func TestOpenAIWSIdentityReplayAcrossWindowsAndTransports(t *testing.T) {
	svc, account, requests := newAffinityWireService(t, false)
	svc.cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 3
	svc.cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 3
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	payload := func(thread, window, turn string, rounds int) []byte {
		input := []any{map[string]any{"role": "user", "content": "first"}}
		for i := 1; i < rounds; i++ {
			input = append(input, map[string]any{"role": "assistant", "content": "answer"}, map[string]any{"role": "user", "content": "next"})
		}
		body, err := json.Marshal(map[string]any{
			"type": "response.create", "model": "gpt-5.1", "instructions": "system", "input": input,
			"client_metadata": map[string]any{"session_id": "shared-session", "thread_id": thread, "x-codex-window-id": thread + ":" + window, "turn_id": turn},
		})
		require.NoError(t, err)
		return body
	}
	headers := func(thread, window, turn string) http.Header {
		h := http.Header{}
		h.Set("session-id", "shared-session")
		h.Set("thread-id", thread)
		h.Set("x-codex-window-id", thread+":"+window)
		h.Set("x-client-request-id", turn)
		return h
	}
	sendHTTP := func(thread, window, turn string, rounds int) []byte {
		c := affinityTestContext(1)
		c.Request.Header = headers(thread, window, turn)
		result, err := svc.Forward(ctx, c, account, payload(thread, window, turn, rounds))
		require.NoError(t, err)
		require.Equal(t, "response.completed", result.UpstreamTerminalEvent)
		return <-requests
	}
	require.Empty(t, gjson.GetBytes(sendHTTP("thread-a", "0", "turn-a1", 1), "previous_response_id").String())
	require.Empty(t, gjson.GetBytes(sendHTTP("thread-b", "0", "turn-b1", 1), "previous_response_id").String(), "another window must start its own context even with identical messages")
	second := sendHTTP("thread-a", "0", "turn-a2", 2)
	require.Equal(t, "resp_affinity_1", gjson.GetBytes(second, "previous_response_id").String())
	require.Len(t, gjson.GetBytes(second, "input").Array(), 1)

	serverErrors := make(chan error, 1)
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
	defer ingress.Close()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(ingress.URL, "http"), &coderws.DialOptions{HTTPHeader: headers("thread-a", "0", "connection-a2")})
	require.NoError(t, err)
	defer client.CloseNow()
	require.NoError(t, client.Write(ctx, coderws.MessageText, payload("thread-a", "0", "turn-a3", 3)))
	_, event, err := client.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String())
	third := <-requests
	require.Equal(t, "resp_affinity_3", gjson.GetBytes(third, "previous_response_id").String(), "HTTP to WS must retain the original thread's continuation")
	require.Len(t, gjson.GetBytes(third, "input").Array(), 1)
	require.NoError(t, client.Close(coderws.StatusNormalClosure, "reconnect"))
	require.NoError(t, <-serverErrors)

	fourth := sendHTTP("thread-a", "0", "turn-a4", 4)
	require.Equal(t, "resp_affinity_4", gjson.GetBytes(fourth, "previous_response_id").String(), "WS to HTTP must resume the same live upstream context")
	require.Len(t, gjson.GetBytes(fourth, "input").Array(), 1)
	windowB := sendHTTP("thread-b", "0", "turn-b2", 2)
	require.Equal(t, "resp_affinity_2", gjson.GetBytes(windowB, "previous_response_id").String(), "window A must not replace window B's response chain")
	child := sendHTTP("child-thread", "0", "turn-child", 2)
	require.Empty(t, gjson.GetBytes(child, "previous_response_id").String(), "a subthread must not borrow its parent's prefix")
	require.Len(t, gjson.GetBytes(child, "input").Array(), 3)
	rollover := sendHTTP("thread-a", "1", "turn-rollover", 5)
	require.Empty(t, gjson.GetBytes(rollover, "previous_response_id").String(), "compaction changes the context window, not the thread")
	require.Len(t, gjson.GetBytes(rollover, "input").Array(), 9)
}
