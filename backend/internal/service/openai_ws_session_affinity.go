package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const openAIWSSessionAffinityTTL = 10 * time.Minute

func (c *openAIWSConn) hasSessionAffinity(now time.Time) bool {
	return c != nil && c.sessionAffinityExpiresAt.Load() > now.UnixNano()
}

// At spare capacity preserve live contexts and grow the pool. At capacity an
// idle slot can be replaced, choosing the least recently used one. Busy slots
// remain subject to the pool's bounded per-connection queue.
func (p *openAIWSConnPool) pickSessionSlotLocked(ap *openAIWSAccountPool, compatibility openAIWSHandshakeCompatibilityKey, routingAffinity string, atCapacity bool) *openAIWSConn {
	var best *openAIWSConn
	bestScore := 3
	now := time.Now()
	for _, conn := range ap.conns {
		if conn == nil || !conn.matchesHandshakeCompatibility(compatibility) || conn.isLeased() || conn.waiters.Load() > 0 || p.isConnPinnedLocked(ap, conn.id) {
			continue
		}
		bound := conn.hasSessionAffinity(now)
		if bound && !atCapacity {
			continue
		}
		score := 1
		if bound {
			score = 2
		} else if conn.matchesRoutingAffinity(routingAffinity) {
			score = 0
		}
		if best == nil || score < bestScore || (score == bestScore && conn.lastUsedAt().Before(best.lastUsedAt())) {
			best = conn
			bestScore = score
		}
	}
	return best
}

type openAIWSSessionIdentity struct {
	groupID, apiKeyID int64
	scope             string
	head              [sha256.Size]byte
}

type openAIWSResponseAffinityKey struct {
	groupID, apiKeyID int64
	scope, responseID string
}

// Only the latest completed context on a socket is eligible for incremental
// sends. Store digests instead of retaining potentially large client histories.
type openAIWSSessionAffinity struct {
	identity   openAIWSSessionIdentity
	accountID  int64
	conn       *openAIWSConn
	responseID string
	settings   [sha256.Size]byte
	input      [][sha256.Size]byte
	expiresAt  time.Time
}

func (b *openAIWSSessionAffinity) responseKey() openAIWSResponseAffinityKey {
	return openAIWSResponseAffinityKey{b.identity.groupID, b.identity.apiKeyID, b.identity.scope, b.responseID}
}

func (b *openAIWSSessionAffinity) active(now time.Time) bool {
	return b != nil && now.Before(b.expiresAt) && b.conn != nil && !b.conn.isClosed() && !b.conn.isUnusable()
}

// The three indexes form one process-local session table. Replacing a socket's
// context removes its old head/response indexes; eviction and expiry do likewise.
type openAIWSSessionAffinityStore struct {
	mu        sync.Mutex
	heads     map[openAIWSSessionIdentity]*openAIWSSessionAffinity
	responses map[openAIWSResponseAffinityKey]*openAIWSSessionAffinity
	conns     map[*openAIWSConn]*openAIWSSessionAffinity
}

func (s *openAIWSSessionAffinityStore) deleteLocked(b *openAIWSSessionAffinity) {
	if b == nil {
		return
	}
	if s.heads[b.identity] == b {
		delete(s.heads, b.identity)
	}
	if s.responses[b.responseKey()] == b {
		delete(s.responses, b.responseKey())
	}
	if s.conns[b.conn] == b {
		delete(s.conns, b.conn)
		b.conn.sessionAffinityExpiresAt.Store(0)
	}
}

func (s *openAIWSSessionAffinityStore) forget(conn *openAIWSConn) {
	s.mu.Lock()
	s.deleteLocked(s.conns[conn])
	s.mu.Unlock()
}

func (s *openAIWSSessionAffinityStore) cleanup(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.conns {
		if !b.active(now) {
			s.deleteLocked(b)
		}
	}
}

func (s *openAIWSSessionAffinityStore) lookup(identity openAIWSSessionIdentity, previousID string) *openAIWSSessionAffinity {
	// Anonymous direct service callers cannot establish a reusable client identity.
	if identity.apiKeyID <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var b *openAIWSSessionAffinity
	if previousID != "" {
		b = s.responses[openAIWSResponseAffinityKey{identity.groupID, identity.apiKeyID, identity.scope, previousID}]
	} else if identity.head != ([sha256.Size]byte{}) {
		b = s.heads[identity]
	}
	if !b.active(time.Now()) {
		s.deleteLocked(b)
		return nil
	}
	return b
}

func (s *openAIWSSessionAffinityStore) bind(b *openAIWSSessionAffinity, responseID string, output []json.RawMessage) {
	if b == nil || b.identity.apiKeyID <= 0 || responseID == "" || b.conn == nil || b.conn.isClosed() || b.conn.isUnusable() {
		return
	}
	b.responseID = responseID
	b.input = append(slices.Clone(b.input), openAIWSSessionItemDigests(output)...)
	b.expiresAt = time.Now().Add(openAIWSSessionAffinityTTL)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.heads == nil {
		s.heads = make(map[openAIWSSessionIdentity]*openAIWSSessionAffinity)
		s.responses = make(map[openAIWSResponseAffinityKey]*openAIWSSessionAffinity)
		s.conns = make(map[*openAIWSConn]*openAIWSSessionAffinity)
	}
	s.deleteLocked(s.conns[b.conn])
	if b.identity.head != ([sha256.Size]byte{}) {
		s.heads[b.identity] = b
	}
	s.responses[b.responseKey()] = b
	s.conns[b.conn] = b
	b.conn.sessionAffinityExpiresAt.Store(b.expiresAt.UnixNano())
}

func openAIWSSessionIdentityForRequest(c *gin.Context, body []byte) openAIWSSessionIdentity {
	key := openAIWSSessionIdentity{groupID: getOpenAIGroupIDFromContext(c), apiKeyID: getAPIKeyIDFromContext(c)}
	key.scope, _ = resolveOpenAIWSExecutionScope(c, body, key.apiKeyID)
	input := gjson.GetBytes(body, "input")
	var prefix []json.RawMessage
	anchored := false
	if input.Type == gjson.String && strings.TrimSpace(input.String()) != "" {
		prefix = []json.RawMessage{json.RawMessage(input.Raw)}
		anchored = true
	} else if input.IsArray() {
		for _, item := range input.Array() {
			role := item.Get("role").String()
			if role != "system" && role != "developer" && role != "user" && item.Get("type").String() != "input_text" {
				break
			}
			prefix = append(prefix, json.RawMessage(item.Raw))
			if role == "user" || item.Get("type").String() == "input_text" {
				anchored = hasMeaningfulOpenAIContent(item.Get("content")) || strings.TrimSpace(item.Get("text").String()) != ""
				break
			}
		}
	}
	if anchored {
		head, _ := json.Marshal([]any{gjson.GetBytes(body, "model").String(), gjson.GetBytes(body, "instructions").String(), openAIWSSessionItemDigests(prefix)})
		key.head = sha256.Sum256(head)
	}
	return key
}

func openAIWSSessionItemDigests(items []json.RawMessage) [][sha256.Size]byte {
	digests := make([][sha256.Size]byte, 0, len(items))
	for _, raw := range items {
		var item map[string]any
		if value := gjson.ParseBytes(raw); value.Type == gjson.String {
			item = map[string]any{"role": "user", "content": value.String()}
			raw, _ = json.Marshal(item)
		}
		if decodeOpenAIJSONUseNumber(raw, &item) == nil && item != nil {
			// Message IDs/status and empty output annotations are transport metadata.
			// Clients commonly omit them when echoing assistant messages as input.
			itemType, _ := item["type"].(string)
			if _, known := openAIResponsesInputItemIDPrefix(itemType); known {
				delete(item, "id")
				delete(item, "status")
			}
			if role, _ := item["role"].(string); role != "" {
				delete(item, "id")
				delete(item, "status")
				item["type"] = "message"
				if content, ok := item["content"].(string); ok {
					item["content"] = []any{map[string]any{"type": "text", "text": content}}
				} else if content, ok := item["content"].([]any); ok {
					for _, part := range content {
						if part, ok := part.(map[string]any); ok && (part["type"] == "input_text" || part["type"] == "output_text") {
							part["type"] = "text"
							if annotations, ok := part["annotations"].([]any); ok && len(annotations) == 0 {
								delete(part, "annotations")
							}
						}
					}
				}
			}
			if normalized, err := json.Marshal(item); err == nil {
				raw = normalized
			}
		}
		digests = append(digests, sha256.Sum256(raw))
	}
	return digests
}

// prepare runs while holding the actual socket lease. A routing lookup alone
// is insufficient: a queued request may find that another turn replaced it.
// The caller retains its original full payload for reconnect/replay.
func (s *openAIWSSessionAffinityStore) prepare(lease *openAIWSConnLease, identity openAIWSSessionIdentity, payload []byte) ([]byte, *openAIWSSessionAffinity, bool) {
	items, exists, err := openAIWSExtractNormalizedInputSequence(payload)
	if err != nil || !exists || lease == nil || lease.conn == nil || identity.apiKeyID <= 0 {
		return payload, nil, false
	}
	settings, err := normalizeOpenAIWSPayloadWithoutInputAndPreviousResponseID(payload)
	if err != nil {
		return payload, nil, false
	}
	// A Codex context-window rollover deliberately starts a new root even if
	// the replayed messages happen to retain the old prefix.
	settings, _ = json.Marshal([]any{json.RawMessage(settings), openAIWSPayloadCodexWindowID(payload)})
	digests := openAIWSSessionItemDigests(items)
	pending := &openAIWSSessionAffinity{identity: identity, accountID: lease.accountID, conn: lease.conn, input: digests, settings: sha256.Sum256(settings), expiresAt: time.Now().Add(openAIWSSessionAffinityTTL)}
	previousID := openAIWSPayloadStringFromRaw(payload, "previous_response_id")
	previous := s.lookup(identity, previousID)
	if previous == nil || previous.conn != lease.conn || previous.settings != pending.settings {
		if previousID != "" {
			return payload, nil, false // The full ancestor context is unknown locally.
		}
		return payload, pending, false
	}
	if len(digests) >= len(previous.input) && (len(digests) > len(previous.input) || previousID != "") && slices.Equal(digests[:len(previous.input)], previous.input) {
		updated, err := setOpenAIWSPayloadInputSequence(payload, items[len(previous.input):], true)
		if err == nil {
			updated, err = setPreviousResponseIDToRawPayload(updated, previous.responseID)
		}
		if err == nil {
			return updated, pending, true
		}
	} else if previousID != "" {
		if identity.head != ([sha256.Size]byte{}) && identity.head == previous.identity.head {
			return payload, nil, false // An edited full history is not a delta.
		}
		// An explicit continuation already contains only the delta. Keep its head
		// so a later full-history request can find the same conversation again.
		pending.identity = previous.identity
		pending.input = append(slices.Clone(previous.input), digests...)
		return payload, pending, false
	}
	return payload, pending, false
}

func openAIWSSessionCompletedOutput(message []byte, eventType string) ([]json.RawMessage, bool) {
	if eventType != "response.completed" && eventType != "response.done" {
		return nil, false
	}
	status := gjson.GetBytes(message, "response.status").String()
	if status != "" && status != "completed" {
		return nil, false
	}
	if responseError := gjson.GetBytes(message, "response.error"); responseError.Exists() && responseError.Type != gjson.Null {
		return nil, false
	}
	output := gjson.GetBytes(message, "response.output")
	if !output.IsArray() {
		return nil, false
	}
	values := output.Array()
	items := make([]json.RawMessage, 0, len(values))
	for _, item := range values {
		items = append(items, json.RawMessage(item.Raw))
	}
	return items, true
}

type openAIWSSessionAffinityContextKey struct{}

func openAIWSSessionAffinityAccountID(ctx context.Context, groupID int64) int64 {
	if ctx != nil {
		if binding, ok := ctx.Value(openAIWSSessionAffinityContextKey{}).(*openAIWSSessionAffinity); ok && binding.identity.groupID == groupID && binding.active(time.Now()) {
			return binding.accountID
		}
	}
	return 0
}

// WithOpenAIWSSessionAffinity carries a local live-socket account preference to
// the normal scheduler, whose eligibility, exclusions and limits still apply.
func (s *OpenAIGatewayService) WithOpenAIWSSessionAffinity(ctx context.Context, c *gin.Context, body []byte) context.Context {
	if s == nil || ctx == nil || (s.cfg != nil && !s.cfg.Gateway.OpenAIWS.Enabled) {
		return ctx
	}
	pool := s.getOpenAIWSConnPool()
	identity := openAIWSSessionIdentityForRequest(c, body)
	b := pool.sessionAffinities.lookup(identity, strings.TrimSpace(gjson.GetBytes(body, "previous_response_id").String()))
	if b == nil {
		return ctx
	}
	return context.WithValue(ctx, openAIWSSessionAffinityContextKey{}, b)
}
