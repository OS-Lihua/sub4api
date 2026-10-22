package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestCodexIdentityLifecycleReplay(t *testing.T) {
	for _, mode := range []string{"off", "device", "session", "full"} {
		t.Run(mode, func(t *testing.T) {
			svc, account, requests := newAffinityWireService(t, false)
			upstreamURL := account.GetCredential("base_url")
			svc.cfg.Gateway.OpenAIWS.OAuthEnabled = true
			svc.cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 3
			svc.cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 3
			svc.getOpenAIWSConnPool().setClientDialerForTest(codexWireSocketDialer{
				url: "ws" + strings.TrimPrefix(upstreamURL, "http"), inner: newDefaultOpenAIWSClientDialer(),
			})
			account.Type = AccountTypeOAuth
			account.Credentials = map[string]any{"access_token": "test-token", "chatgpt_account_id": "identity-replay-account"}
			account.Extra = map[string]any{
				"openai_oauth_responses_websockets_v2_enabled": true, "openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModeCtxPool,
				codexFingerprintModeExtraKey: mode, codexFingerprintSeedExtraKey: "8c3ec394-50af-42ca-9ac4-1b4121fcde53",
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			forward := func(thread, window, turn string, followup bool) []byte {
				metadata := map[string]any{"installation_id": "installation", "session_id": "shared-session", "thread_id": thread, "window_id": thread + ":" + window, "turn_id": turn, "request_kind": "turn"}
				turnMetadata, err := json.Marshal(metadata)
				require.NoError(t, err)
				var body map[string]any
				original := affinityFirstPayload
				if followup {
					original = affinityNextPayload
				}
				require.NoError(t, json.Unmarshal([]byte(original), &body))
				body["client_metadata"] = map[string]any{
					"installation_id": "installation", "session_id": "shared-session", "thread_id": thread,
					"x-codex-window-id": thread + ":" + window, openAIWSTurnMetadataHeader: string(turnMetadata),
				}
				raw, err := json.Marshal(body)
				require.NoError(t, err)
				c := affinityTestContext(1)
				for name, value := range map[string]string{
					"x-codex-installation-id": "installation", "session-id": "shared-session", "thread-id": thread,
					"x-codex-window-id": thread + ":" + window, "x-client-request-id": turn,
					openAIWSTurnMetadataHeader: string(turnMetadata),
				} {
					c.Request.Header.Set(name, value)
				}
				result, err := svc.Forward(ctx, c, account, raw)
				require.NoError(t, err)
				require.Equal(t, "response.completed", result.UpstreamTerminalEvent)
				got := <-requests
				// Build the HTTP variant from the same attempt's staged identity.
				httpRequest, err := svc.buildUpstreamRequest(ctx, c, account, got, "test-token", true, "shared-session", true)
				require.NoError(t, err)
				for _, pair := range [][2]string{{"session-id", "session_id"}, {"thread-id", "thread_id"}, {"x-codex-window-id", "x-codex-window-id"}} {
					require.Equal(t, gjson.GetBytes(got, "client_metadata."+pair[1]).String(), httpRequest.Header.Get(pair[0]), "HTTP/WS identity parity for "+pair[0])
				}
				return got
			}
			first := forward("thread-a", "0", "turn-1", false)
			second := forward("thread-a", "0", "turn-2", true)
			for _, field := range []string{"installation_id", "session_id", "thread_id", "x-codex-window-id"} {
				require.NotEmpty(t, gjson.GetBytes(first, "client_metadata."+field).String(), field)
				require.Equal(t, gjson.GetBytes(first, "client_metadata."+field).String(), gjson.GetBytes(second, "client_metadata."+field).String(), field+" must survive another turn/reconnect")
			}
			metadataOf := func(raw []byte) gjson.Result {
				return gjson.Parse(gjson.GetBytes(raw, "client_metadata."+openAIWSTurnMetadataHeader).String())
			}
			require.NotEqual(t, metadataOf(first).Get("turn_id").String(), metadataOf(second).Get("turn_id").String())
			require.Equal(t, "resp_affinity_1", gjson.GetBytes(second, "previous_response_id").String(), "turn metadata must not break incremental reuse")
			other := forward("thread-b", "0", "turn-3", false)
			require.NotEqual(t, gjson.GetBytes(first, "client_metadata.thread_id").String(), gjson.GetBytes(other, "client_metadata.thread_id").String(), "windows must retain distinct threads in every opt-in mode")
			require.Empty(t, gjson.GetBytes(other, "previous_response_id").String())
			rollover := forward("thread-a", "1", "turn-4", true)
			require.Equal(t, gjson.GetBytes(first, "client_metadata.thread_id").String(), gjson.GetBytes(rollover, "client_metadata.thread_id").String())
			require.NotEqual(t, gjson.GetBytes(first, "client_metadata.x-codex-window-id").String(), gjson.GetBytes(rollover, "client_metadata.x-codex-window-id").String())
			require.Empty(t, gjson.GetBytes(rollover, "previous_response_id").String())
		})
	}
}
