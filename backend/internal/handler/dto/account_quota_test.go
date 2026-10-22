package dto

import (
	"testing"
	"time"

	"github.com/MACOS-DO/sub4api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpenAISubscriptionAccountQuotaDTO(t *testing.T) {
	for _, accountType := range []string{service.AccountTypeOAuth, service.AccountTypeSetupToken} {
		t.Run(accountType, func(t *testing.T) {
			account := &service.Account{ID: 3, Platform: service.PlatformOpenAI, Type: accountType,
				Extra: map[string]any{"quota_limit": 100.0, "quota_used": 20.0,
					"quota_daily_limit": 10.0, "quota_daily_used": 10.0, "quota_daily_start": time.Now().Add(-25 * time.Hour).Format(time.RFC3339)}}
			got := AccountFromService(account)
			require.NotNil(t, got.QuotaLimit)
			require.Equal(t, 100.0, *got.QuotaLimit)
			require.Equal(t, 20.0, *got.QuotaUsed)
			require.Equal(t, 10.0, *got.QuotaDailyLimit)
			require.Zero(t, *got.QuotaDailyUsed, "expired spending windows should render as available")
		})
	}
}
