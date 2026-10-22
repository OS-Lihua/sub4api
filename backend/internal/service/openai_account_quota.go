package service

import (
	"context"
	"errors"
	"fmt"
)

var ErrAccountQuotaExceeded = errors.New("account quota exceeded")

// CheckOpenAIAccountQuota rechecks persisted budgets before a WS turn. A live
// connection can outlast its quota or an admin's quota configuration changes;
// its handshake account and the scheduler snapshot are therefore insufficient.
func (s *OpenAIGatewayService) CheckOpenAIAccountQuota(ctx context.Context, account *Account) (*Account, error) {
	if !account.SupportsQuotaLimits() && !account.IsOpenAIOAuthLike() {
		return account, nil
	}
	latest := account
	if s.accountRepo != nil {
		var err error
		latest, err = s.accountRepo.GetByID(ctx, account.ID)
		if err != nil {
			return nil, fmt.Errorf("read account spending quota: %w", err)
		}
		if latest == nil {
			return nil, ErrAccountNotFound
		}
	}
	if latest.SupportsQuotaLimits() && latest.IsQuotaExceeded() {
		return latest, ErrAccountQuotaExceeded
	}
	// Apply the same configured 5h/7d usage thresholds as fresh HTTP selection.
	// Long-lived connections otherwise bypass auto-pause until reconnecting.
	if paused, _ := shouldAutoPauseOpenAIAccountByQuota(s.withOpenAIQuotaAutoPauseContext(ctx), latest); paused {
		return latest, ErrAccountQuotaExceeded
	}
	return latest, nil
}
