package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MACOS-DO/sub4api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const affinityFirstPayload = `{"type":"response.create","model":"gpt-5.1","instructions":"system","input":[{"role":"user","content":"first"}]}`
const affinityNextPayload = `{"type":"response.create","model":"gpt-5.1","instructions":"system","input":[{"role":"user","content":"first"},{"role":"assistant","content":"answer"},{"role":"user","content":"next"}]}`
const affinityOutput = `{"id":"msg_answer","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"answer","annotations":[]}]}`

func affinityTestContext(apiKeyID int64) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	groupID := int64(10)
	c.Set("api_key", &APIKey{ID: apiKeyID, GroupID: &groupID})
	return c
}

func bindAffinityTestTurn(t *testing.T, store *openAIWSSessionAffinityStore, lease *openAIWSConnLease, c *gin.Context, payload, responseID string) {
	t.Helper()
	_, pending, _ := store.prepare(lease, openAIWSSessionIdentityForRequest(c, []byte(payload)), []byte(payload))
	require.NotNil(t, pending)
	store.bind(pending, responseID, []json.RawMessage{json.RawMessage(affinityOutput)})
}

func TestOpenAIWSSessionAffinity_HeadAndCallerIsolation(t *testing.T) {
	c := affinityTestContext(1)
	first := openAIWSSessionIdentityForRequest(c, []byte(affinityFirstPayload))
	next := openAIWSSessionIdentityForRequest(c, []byte(affinityNextPayload))
	require.Equal(t, first, next)
	require.NotEqual(t, first, openAIWSSessionIdentityForRequest(affinityTestContext(2), []byte(affinityFirstPayload)))
	c.Request.Header.Set("thread-id", "another-thread")
	require.NotEqual(t, first, openAIWSSessionIdentityForRequest(c, []byte(affinityFirstPayload)))
	c.Request.Header.Del("thread-id")
	changed := []byte(`{"model":"gpt-5.1","instructions":"system","input":[{"role":"user","content":"another root"}]}`)
	require.NotEqual(t, first, openAIWSSessionIdentityForRequest(c, changed))
	before := []byte(`{"model":"gpt-5.1","input":[{"role":"system","content":"a"},{"role":"user","content":"first"}]}`)
	after := []byte(`{"model":"gpt-5.1","input":[{"role":"system","content":"a"},{"role":"user","content":"first"},{"role":"developer","content":"later"}]}`)
	require.Equal(t, openAIWSSessionIdentityForRequest(c, before), openAIWSSessionIdentityForRequest(c, after), "head ends at the first user")
}

func TestOpenAIWSSessionAffinity_FullHistoryBecomesDelta(t *testing.T) {
	c := affinityTestContext(1)
	store := &openAIWSSessionAffinityStore{}
	lease := &openAIWSConnLease{accountID: 9, conn: newOpenAIWSConn("cached", 9, nil, nil)}
	bindAffinityTestTurn(t, store, lease, c, affinityFirstPayload, "resp_first")
	identity := openAIWSSessionIdentityForRequest(c, []byte(affinityNextPayload))
	wire, pending, changed := store.prepare(lease, identity, []byte(affinityNextPayload))
	require.True(t, changed)
	require.Equal(t, "resp_first", gjson.GetBytes(wire, "previous_response_id").String())
	require.Equal(t, "next", gjson.GetBytes(wire, "input.0.content").String())
	require.Len(t, gjson.GetBytes(wire, "input").Array(), 1)
	require.Len(t, pending.input, 3)
	require.False(t, gjson.Get(affinityNextPayload, "previous_response_id").Exists(), "original replay payload is untouched")
	store.bind(pending, "resp_second", []json.RawMessage{json.RawMessage(affinityOutput)})
	require.Nil(t, store.lookup(identity, "resp_first"), "only the latest response on this socket is reusable")
	require.NotNil(t, store.lookup(identity, "resp_second"))
	require.Len(t, store.conns, 1)
}

func TestOpenAIWSSessionAffinity_ExplicitDeltaPreservesHead(t *testing.T) {
	c := affinityTestContext(1)
	store := &openAIWSSessionAffinityStore{}
	lease := &openAIWSConnLease{accountID: 9, conn: newOpenAIWSConn("cached", 9, nil, nil)}
	bindAffinityTestTurn(t, store, lease, c, affinityFirstPayload, "resp_first")
	delta := []byte(`{"type":"response.create","model":"gpt-5.1","instructions":"system","previous_response_id":"resp_first","input":[{"role":"user","content":"next"}]}`)
	identity := openAIWSSessionIdentityForRequest(c, delta)
	wire, pending, changed := store.prepare(lease, identity, delta)
	require.False(t, changed)
	require.Equal(t, delta, wire)
	require.Len(t, pending.input, 3)
	store.bind(pending, "resp_second", []json.RawMessage{json.RawMessage(affinityOutput)})
	full := []byte(`{"type":"response.create","model":"gpt-5.1","instructions":"system","input":[{"role":"user","content":"first"},{"role":"assistant","content":"answer"},{"role":"user","content":"next"},{"role":"assistant","content":"answer"},{"role":"user","content":"third"}]}`)
	wire, _, changed = store.prepare(lease, openAIWSSessionIdentityForRequest(c, full), full)
	require.True(t, changed)
	require.Equal(t, "resp_second", gjson.GetBytes(wire, "previous_response_id").String())
	require.Equal(t, "third", gjson.GetBytes(wire, "input.0.content").String())
}

func TestOpenAIWSSessionAffinity_UnsafeMatchesKeepFullHistory(t *testing.T) {
	for _, scenario := range []string{"different_history", "different_settings", "new_context_window", "other_socket", "other_key", "expired", "replaced_while_queued"} {
		t.Run(scenario, func(t *testing.T) {
			c := affinityTestContext(1)
			store := &openAIWSSessionAffinityStore{}
			lease := &openAIWSConnLease{accountID: 9, conn: newOpenAIWSConn("cached", 9, nil, nil)}
			bindAffinityTestTurn(t, store, lease, c, affinityFirstPayload, "resp_first")
			payload := []byte(affinityNextPayload)
			switch scenario {
			case "different_history":
				payload = []byte(`{"type":"response.create","model":"gpt-5.1","instructions":"system","input":[{"role":"user","content":"first"},{"role":"assistant","content":"edited"},{"role":"user","content":"next"}]}`)
			case "different_settings":
				payload = []byte(`{"type":"response.create","model":"gpt-5.1","instructions":"system","temperature":0.2,"input":[{"role":"user","content":"first"},{"role":"assistant","content":"answer"},{"role":"user","content":"next"}]}`)
			case "new_context_window":
				payload = []byte(`{"type":"response.create","model":"gpt-5.1","instructions":"system","client_metadata":{"x-codex-window-id":"new-window"},"input":[{"role":"user","content":"first"},{"role":"assistant","content":"answer"},{"role":"user","content":"next"}]}`)
			case "other_socket":
				lease = &openAIWSConnLease{accountID: 9, conn: newOpenAIWSConn("another", 9, nil, nil)}
			case "other_key":
				c = affinityTestContext(2)
			case "expired":
				store.cleanup(time.Now().Add(openAIWSSessionAffinityTTL))
			case "replaced_while_queued":
				other := `{"type":"response.create","model":"gpt-5.1","input":[{"role":"user","content":"other conversation"}]}`
				bindAffinityTestTurn(t, store, lease, c, other, "resp_other")
			}
			wire, _, changed := store.prepare(lease, openAIWSSessionIdentityForRequest(c, payload), payload)
			require.False(t, changed)
			require.Equal(t, payload, wire)
		})
	}
}

func TestOpenAIWSSessionAffinity_OnlyCompleteResponsesEstablishContext(t *testing.T) {
	for _, event := range []string{
		`{"type":"response.failed","response":{"output":[]}}`,
		`{"type":"response.done","response":{"status":"incomplete","output":[]}}`,
		`{"type":"response.completed","response":{"status":"failed","output":[]}}`,
		`{"type":"response.completed","response":{"status":"completed","error":{"code":"failed"},"output":[]}}`,
		`{"type":"response.completed","response":{"status":"completed"}}`,
	} {
		_, reusable := openAIWSSessionCompletedOutput([]byte(event), gjson.Get(event, "type").String())
		require.False(t, reusable, event)
	}
	_, reusable := openAIWSSessionCompletedOutput([]byte(`{"response":{"status":"completed","output":[]}}`), "response.completed")
	require.True(t, reusable)
}

func TestOpenAIWSSessionAffinity_StringInputAndLocalAccountPreference(t *testing.T) {
	c := affinityTestContext(1)
	pool := newOpenAIWSConnPool(&config.Config{})
	t.Cleanup(pool.Close)
	svc := &OpenAIGatewayService{openaiWSPool: pool}
	lease := &openAIWSConnLease{accountID: 9, conn: newOpenAIWSConn("cached", 9, nil, nil)}
	first := `{"type":"response.create","model":"gpt-5.1","instructions":"system","input":"first"}`
	bindAffinityTestTurn(t, &pool.sessionAffinities, lease, c, first, "resp_first")
	identity := openAIWSSessionIdentityForRequest(c, []byte(affinityNextPayload))
	wire, _, changed := pool.sessionAffinities.prepare(lease, identity, []byte(affinityNextPayload))
	require.True(t, changed)
	require.Equal(t, "resp_first", gjson.GetBytes(wire, "previous_response_id").String())
	ctx := svc.WithOpenAIWSSessionAffinity(context.Background(), c, []byte(affinityNextPayload))
	groupID := int64(10)
	accountID, err := svc.getStickySessionAccountID(ctx, &groupID, "new-header-hash")
	require.NoError(t, err)
	require.Equal(t, int64(9), accountID)
	otherCtx := svc.WithOpenAIWSSessionAffinity(context.Background(), affinityTestContext(2), []byte(affinityNextPayload))
	accountID, err = svc.getStickySessionAccountID(otherCtx, &groupID, "new-header-hash")
	require.NoError(t, err)
	require.Zero(t, accountID)
}

func TestOpenAIWSSessionAffinity_ExpiryAndEvictionRemoveAllIndexes(t *testing.T) {
	for _, evict := range []bool{false, true} {
		t.Run(map[bool]string{false: "expiry", true: "eviction"}[evict], func(t *testing.T) {
			pool := newOpenAIWSConnPool(&config.Config{})
			t.Cleanup(pool.Close)
			c := affinityTestContext(1)
			conn := newOpenAIWSConn("cached", 9, nil, nil)
			pool.getOrCreateAccountPool(9).conns[conn.id] = conn
			lease := &openAIWSConnLease{pool: pool, accountID: 9, conn: conn}
			bindAffinityTestTurn(t, &pool.sessionAffinities, lease, c, affinityFirstPayload, "resp_first")
			require.True(t, conn.hasSessionAffinity(time.Now()))
			if evict {
				pool.evictConn(9, conn.id)
			} else {
				pool.sessionAffinities.cleanup(time.Now().Add(openAIWSSessionAffinityTTL))
			}
			require.False(t, conn.hasSessionAffinity(time.Now()))
			require.Empty(t, pool.sessionAffinities.heads)
			require.Empty(t, pool.sessionAffinities.responses)
			require.Empty(t, pool.sessionAffinities.conns)
		})
	}
}

func TestOpenAIWSSessionAffinity_SlotGrowthLRUAndStickyQueue(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 2
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 2
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 2
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(&openAIWSFakeDialer{})
	t.Cleanup(pool.Close)
	account := &Account{ID: 9, Concurrency: 2}
	req := openAIWSAcquireRequest{Account: account, WSURL: "wss://example.test/responses", PreserveSessionAffinity: true}
	first, err := pool.Acquire(context.Background(), req)
	require.NoError(t, err)
	bindAffinityTestTurn(t, &pool.sessionAffinities, first, affinityTestContext(1), affinityFirstPayload, "resp_first")
	first.Release()
	second, err := pool.Acquire(context.Background(), req)
	require.NoError(t, err)
	require.NotEqual(t, first.ConnID(), second.ConnID(), "grow before replacing an active context")
	bindAffinityTestTurn(t, &pool.sessionAffinities, second, affinityTestContext(2), affinityFirstPayload, "resp_other")
	second.Release()
	first.conn.lastUsedNano.Store(time.Now().Add(-time.Second).UnixNano())
	third, err := pool.Acquire(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, first.ConnID(), third.ConnID(), "at capacity replace the least recently used idle slot")

	stickyReq := req
	stickyReq.PreferredConnID = third.ConnID()
	stickyReq.ForcePreferredConn = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	type result struct {
		lease *openAIWSConnLease
		err   error
	}
	done := make(chan result, 1)
	go func() {
		lease, err := pool.Acquire(ctx, stickyReq)
		done <- result{lease, err}
	}()
	require.Eventually(t, func() bool { return third.conn.waiters.Load() == 1 }, time.Second, time.Millisecond)
	select {
	case got := <-done:
		if got.lease != nil {
			got.lease.Release()
		}
		t.Fatal("sticky continuation drifted to an idle socket")
	default:
	}
	third.Release()
	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, third.ConnID(), got.lease.ConnID())
	got.lease.Release()
}
