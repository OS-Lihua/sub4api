package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MACOS-DO/sub4api/internal/config"
	"github.com/MACOS-DO/sub4api/internal/pkg/tlsfingerprint"
	"github.com/MACOS-DO/sub4api/internal/server/middleware"
	"github.com/MACOS-DO/sub4api/internal/service"
	"github.com/MACOS-DO/sub4api/internal/testutil"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type wsAccountQuotaRepo struct {
	service.AccountRepository
	mu      sync.Mutex
	account service.Account
}

func (r *wsAccountQuotaRepo) GetByID(context.Context, int64) (*service.Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	account := r.account
	account.Extra = maps.Clone(account.Extra)
	return &account, nil
}

func (r *wsAccountQuotaRepo) ListSchedulableByPlatform(ctx context.Context, _ string) ([]service.Account, error) {
	account, err := r.GetByID(ctx, r.account.ID)
	return []service.Account{*account}, err
}

func (r *wsAccountQuotaRepo) ListSchedulableByGroupIDAndPlatform(ctx context.Context, _ int64, platform string) ([]service.Account, error) {
	return r.ListSchedulableByPlatform(ctx, platform)
}

type wsAccountQuotaBillingRepo struct {
	service.UsageBillingRepository
	accounts *wsAccountQuotaRepo
	commands chan *service.UsageBillingCommand
	seen     map[string]bool
	err      error
	panic    bool
}

func (r *wsAccountQuotaBillingRepo) Apply(_ context.Context, cmd *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	r.accounts.mu.Lock()
	defer r.accounts.mu.Unlock()
	if r.seen[cmd.RequestID] {
		return &service.UsageBillingApplyResult{}, nil
	}
	r.commands <- cmd
	if r.panic {
		panic("billing panic")
	}
	if r.err != nil {
		return nil, r.err
	}
	r.seen[cmd.RequestID] = true
	r.accounts.account.Extra["quota_used"] = r.accounts.account.GetQuotaUsed() + cmd.AccountQuotaCost
	return &service.UsageBillingApplyResult{Applied: true}, nil
}

type wsQuotaHTTPUpstream struct{}

func (wsQuotaHTTPUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return http.DefaultClient.Do(req)
}

func (wsQuotaHTTPUpstream) DoWithTLS(req *http.Request, proxy string, accountID int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return wsQuotaHTTPUpstream{}.Do(req, proxy, accountID, concurrency)
}

func TestOpenAIWSAccountQuotaStopsNextTurnBeforeUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{service.OpenAIWSIngressModeCtxPool, service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeHTTPBridge} {
		for _, billingFailure := range []string{"none", "error", "panic"} {
			t.Run(mode+"/billing_"+billingFailure, func(t *testing.T) {
				requests := make(chan []byte, 4)
				var sequence atomic.Int64
				complete := func() []byte {
					return []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_budget_%d","model":"gpt-5.1","status":"completed","output":[],"usage":{"input_tokens":100,"output_tokens":10}}}`, sequence.Add(1)))
				}
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPost {
						body, _ := io.ReadAll(r.Body)
						requests <- body
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: %s\n\n", complete())
						return
					}
					conn, err := coderws.Accept(w, r, nil)
					if err != nil {
						return
					}
					defer conn.CloseNow()
					for {
						_, body, err := conn.Read(r.Context())
						if err != nil {
							return
						}
						requests <- body
						if err := conn.Write(r.Context(), coderws.MessageText, complete()); err != nil {
							return
						}
					}
				}))
				t.Cleanup(upstream.Close)
				groupID := int64(4302)
				repo := &wsAccountQuotaRepo{account: service.Account{
					ID: 9902, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
					Status: service.StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{groupID},
					Credentials: map[string]any{"api_key": "test-key", "base_url": upstream.URL},
					Extra: map[string]any{
						"quota_limit": 0.00000001,
						"openai_apikey_responses_websockets_v2_enabled": true,
						"openai_apikey_responses_websockets_v2_mode":    mode,
					},
				}}
				cfg := &config.Config{RunMode: config.RunModeStandard}
				cfg.Default.RateMultiplier = 1
				cfg.Security.URLAllowlist.AllowInsecureHTTP = true
				cfg.Gateway.OpenAIWS.Enabled = true
				cfg.Gateway.OpenAIWS.APIKeyEnabled = true
				cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
				cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
				cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds = 3
				billingRepo := &wsAccountQuotaBillingRepo{accounts: repo, commands: make(chan *service.UsageBillingCommand, 2), seen: map[string]bool{}}
				if billingFailure == "error" {
					billingRepo.err = errors.New("billing unavailable")
				}
				billingRepo.panic = billingFailure == "panic"
				usageRepo := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 2)}
				cache := &cyberInflightBillingCache{handlerInflightCache: newHandlerInflightCache(100), released: make(chan struct{}, 4)}
				billingCache := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil, cfg, nil)
				t.Cleanup(billingCache.Stop)
				concurrency := service.NewConcurrencyService(&concurrencyCacheMock{
					acquireUserSlotFn:    func(context.Context, int64, int, string) (bool, error) { return true, nil },
					acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
				})
				gateway := service.NewOpenAIGatewayService(repo, usageRepo, billingRepo, nil, nil, nil,
					testutil.NewRedisGatewayCache(t), cfg, nil, concurrency, service.NewBillingService(cfg, nil), nil,
					billingCache, wsQuotaHTTPUpstream{}, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
				t.Cleanup(gateway.CloseOpenAIWSPool)
				h := NewOpenAIGatewayHandler(gateway, concurrency, billingCache, &service.APIKeyService{}, nil, nil, nil, nil, cfg)
				pool := newUsageRecordTestPool(t)
				blocked, release := make(chan struct{}), make(chan struct{})
				defer close(release)
				pool.Submit(func(context.Context) { close(blocked); <-release })
				<-blocked
				h.usageRecordWorkerPool = pool
				apiKey := &service.APIKey{ID: 2, UserID: 1, GroupID: &groupID,
					User:  &service.User{ID: 1, Status: service.StatusActive, Balance: 100},
					Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive, RateMultiplier: 1}}
				done := make(chan struct{})
				router := gin.New()
				router.GET("/v1/responses", func(c *gin.Context) {
					defer close(done)
					c.Set(string(middleware.ContextKeyAPIKey), apiKey)
					c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1, Concurrency: 1})
					h.ResponsesWebSocket(c)
				})
				server := httptest.NewServer(router)
				defer server.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
				require.NoError(t, err)
				defer client.CloseNow()
				body := []byte(`{"type":"response.create","model":"gpt-5.1","input":"hello"}`)
				require.NoError(t, client.Write(ctx, coderws.MessageText, body))
				for {
					_, event, err := client.Read(ctx)
					require.NoError(t, err)
					if gjson.GetBytes(event, "type").String() == "response.completed" {
						break
					}
				}
				<-requests
				require.NoError(t, client.Write(ctx, coderws.MessageText, body))
				_, _, err = client.Read(ctx)
				wantStatus := coderws.StatusTryAgainLater
				if billingFailure != "none" {
					wantStatus = coderws.StatusInternalError
				}
				require.Equal(t, wantStatus, coderws.CloseStatus(err))
				select {
				case cmd := <-billingRepo.commands:
					require.Positive(t, cmd.AccountQuotaCost, "quota settlement must bypass the blocked billing worker")
				case <-ctx.Done():
					t.Fatal("first turn was never settled")
				}
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("quota rejection did not end the client session")
				}
				require.Empty(t, requests, "the exhausted second turn must never reach the provider")
			})
		}
	}
}
