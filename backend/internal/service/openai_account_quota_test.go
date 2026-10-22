package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type openAIQuotaAccountRepo struct {
	AccountRepository
	account *Account
	err     error
}

func (r *openAIQuotaAccountRepo) GetByID(context.Context, int64) (*Account, error) {
	return r.account, r.err
}

func TestOpenAIAccountQuotaSchedulingAndBilling(t *testing.T) {
	for _, accountType := range []string{AccountTypeOAuth, AccountTypeSetupToken, AccountTypeAPIKey} {
		for _, dimension := range []string{"", "daily_", "weekly_"} {
			t.Run(accountType+"/"+dimension, func(t *testing.T) {
				account := &Account{
					ID: 3, Platform: PlatformOpenAI, Type: accountType,
					Status: StatusActive, Schedulable: true,
					Credentials: map[string]any{"plan_type": "pro"},
					Extra: map[string]any{
						"quota_" + dimension + "limit": 10.0,
						"quota_" + dimension + "used":  10.0,
						"quota_" + dimension + "start": time.Now().Add(-time.Hour).Format(time.RFC3339),
					},
				}
				require.False(t, account.IsSchedulable(), "HTTP and sticky selection must reject an exhausted subscription budget")
				require.False(t, (&GatewayService{}).isAccountSchedulableForQuota(account))
				params := &postUsageBillingParams{
					Account: account, User: &User{ID: 1}, APIKey: &APIKey{ID: 2},
					Cost: &CostBreakdown{TotalCost: 2, ActualCost: 0.1}, AccountRateMultiplier: 1.5,
				}
				cmd := buildUsageBillingCommand("resp_budget", nil, params)
				require.Equal(t, 3.0, cmd.AccountQuotaCost, "account budgets use upstream cost and account multiplier, independent of customer pricing")
				if dimension != "" {
					account.Extra["quota_"+dimension+"start"] = time.Now().Add(-8 * 24 * time.Hour).Format(time.RFC3339)
					require.True(t, account.IsSchedulable(), "expired quota periods must become eligible again")
				}
			})
		}
	}
}

func TestOpenAIAccountQuotaRefreshesBetweenTurns(t *testing.T) {
	initial := &Account{ID: 3, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	latest := *initial
	latest.Extra = map[string]any{"quota_limit": 10.0, "quota_used": 10.0}
	repo := &openAIQuotaAccountRepo{account: &latest}
	svc := &OpenAIGatewayService{accountRepo: repo}
	_, err := svc.CheckOpenAIAccountQuota(context.Background(), initial)
	require.ErrorIs(t, err, ErrAccountQuotaExceeded, "newly configured/exhausted budgets must apply to an already open WS")
	require.Nil(t, initial.Extra, "the handshake snapshot is immutable")

	reset := latest
	reset.Extra = map[string]any{"quota_daily_limit": 10.0, "quota_daily_used": 10.0, "quota_daily_start": time.Now().Add(-25 * time.Hour).Format(time.RFC3339)}
	repo.account = &reset
	fresh, err := svc.CheckOpenAIAccountQuota(context.Background(), &latest)
	require.NoError(t, err)
	require.Same(t, &reset, fresh, "billing must receive the fresh budget configuration")

	repo.err = errors.New("database unavailable")
	_, err = svc.CheckOpenAIAccountQuota(context.Background(), initial)
	require.ErrorIs(t, err, repo.err, "quota read failures must not permit untracked turns")
	repo.err, repo.account = nil, nil
	_, err = svc.CheckOpenAIAccountQuota(context.Background(), initial)
	require.ErrorIs(t, err, ErrAccountNotFound)
}

func TestOpenAIOAuthQuotaRecordUsageForHTTPAndWS(t *testing.T) {
	for _, ws := range []bool{false, true} {
		t.Run(map[bool]string{false: "http", true: "ws"}[ws], func(t *testing.T) {
			usageRepo := &openAIRecordUsageLogRepoStub{inserted: true}
			billingRepo := &openAIRecordUsageBillingRepoStub{}
			svc := newOpenAIRecordUsageServiceWithBillingRepoForTest(usageRepo, billingRepo, nil, nil, nil)
			account := &Account{ID: 3, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				Credentials: map[string]any{"plan_type": "pro"}, Extra: map[string]any{"quota_daily_limit": 10.0}}
			err := svc.RecordUsage(context.Background(), &OpenAIRecordUsageInput{
				Result: &OpenAIForwardResult{RequestID: "resp_pro_budget", OpenAIWSMode: ws, Model: "gpt-5.1",
					Usage: OpenAIUsage{InputTokens: 100, OutputTokens: 10}, Duration: time.Second},
				Account: account, User: &User{ID: 1}, APIKey: &APIKey{ID: 2},
			})
			require.NoError(t, err)
			require.Equal(t, 1, billingRepo.calls)
			require.Equal(t, AccountTypeOAuth, billingRepo.lastCmd.AccountType)
			require.Positive(t, billingRepo.lastCmd.AccountQuotaCost, "both ingress transports must send the OAuth account cost to idempotent settlement")
			require.Positive(t, billingRepo.lastCmd.BalanceCost, "customer billing remains independent")
		})
	}
}

func TestOpenAIAccountQuotaRechecksUpstreamWindows(t *testing.T) {
	for _, window := range []string{"5h", "7d"} {
		t.Run(window, func(t *testing.T) {
			account := &Account{ID: 3, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				Extra: map[string]any{"auto_pause_" + window + "_threshold": 0.8, "codex_" + window + "_used_percent": 90.0}}
			svc := &OpenAIGatewayService{accountRepo: &openAIQuotaAccountRepo{account: account}}
			_, err := svc.CheckOpenAIAccountQuota(context.Background(), account)
			require.ErrorIs(t, err, ErrAccountQuotaExceeded)
			account.Extra["auto_pause_"+window+"_disabled"] = true
			_, err = svc.CheckOpenAIAccountQuota(context.Background(), account)
			require.NoError(t, err, "explicit per-account exemptions must still be honored")
		})
	}
}

func TestOpenAIAccountQuotaPreservesShadowQuotaOwnership(t *testing.T) {
	parentID := int64(1)
	shadow := &Account{ID: 3, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		ParentAccountID: &parentID, Status: StatusActive, Schedulable: true,
		Extra: map[string]any{"quota_limit": 10.0, "quota_used": 10.0}}
	require.False(t, shadow.SupportsQuotaLimits(), "credential shadows use their own upstream quota dimension, not local monetary budgets")
	require.True(t, shadow.IsSchedulable())
	svc := &OpenAIGatewayService{}
	_, err := svc.CheckOpenAIAccountQuota(context.Background(), shadow)
	require.NoError(t, err)
	params := &postUsageBillingParams{Account: shadow, User: &User{ID: 1}, APIKey: &APIKey{ID: 2},
		Cost: &CostBreakdown{TotalCost: 2, ActualCost: 2}, AccountRateMultiplier: 1}
	require.Zero(t, buildUsageBillingCommand("resp_shadow", nil, params).AccountQuotaCost)
}
