package gateway

import (
	"crypto/sha256"
	"sync"
	"time"
)

// Provider prompt caching, decided here and applied in adapter.go.
//
// Anthropic bills a cache read at roughly a tenth of an input token and a cache
// write at roughly 1.25x. That asymmetry is the whole design problem: marking
// every request for caching is not free, it is a 25% surcharge on any prefix
// that never gets reused. A gateway that marks blindly makes one-shot traffic
// more expensive and calls it an optimisation.
//
// So nothing is marked on first sight. A system prompt is marked only once the
// same one has been seen again inside the cache's own lifetime, which is the
// only evidence available that it is a prefix rather than a one-off. The first
// request pays nothing extra, the second pays the write, and everything after it
// inside the window reads.
//
// This is a decision a gateway is unusually well placed to make: it sees every
// request across every caller, where an individual SDK sees one conversation.
type prefixCache struct {
	mu   sync.Mutex
	seen map[[32]byte]time.Time
	ttl  time.Duration
	max  int
}

// promptCacheTTL is Anthropic's ephemeral cache lifetime. Tracking for longer
// would mark a prefix whose cache has already expired, which buys a write and
// no reads -- the exact surcharge this is built to avoid.
const promptCacheTTL = 5 * time.Minute

// promptCacheMaxTracked bounds the table. System prompts are caller-supplied, so
// the key space is unbounded by construction and an unbounded map here is a
// memory leak with a very slow fuse. At the cap the table is emptied rather than
// evicted one at a time: this is a cache of a cache, everything in it expires in
// minutes anyway, and an eviction policy would be more machinery than the thing
// is worth.
const promptCacheMaxTracked = 4096

// promptCacheMinBytes is roughly the smallest system prompt worth marking.
// Anthropic ignores a cache_control block below its minimum cacheable length --
// 1024 tokens on the larger models, more on the small ones -- so marking a short
// prompt is not harmful, just pointless. Four bytes per token is the usual rough
// conversion, and this is deliberately above the 1024-token line so that a
// prompt which is marked has a real chance of actually being cached.
const promptCacheMinBytes = 6000

func newPrefixCache() *prefixCache {
	return &prefixCache{seen: map[[32]byte]time.Time{}, ttl: promptCacheTTL, max: promptCacheMaxTracked}
}

// worthCaching records this system prompt and reports whether it has been seen
// before inside the TTL -- which is to say, whether it is worth paying a cache
// write for.
//
// Hashed rather than stored: a system prompt is caller content, and docs/
// SECURITY.md promises prompt text does not appear in product telemetry or in
// gateway state that outlives the request. A 32-byte digest is enough to answer
// "the same as last time" and carries nothing back.
func (p *prefixCache) worthCaching(system string) bool {
	if p == nil || len(system) < promptCacheMinBytes {
		return false
	}
	key := sha256.Sum256([]byte(system))
	now := time.Now()

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.seen) >= p.max {
		p.seen = map[[32]byte]time.Time{}
	}
	last, ok := p.seen[key]
	p.seen[key] = now
	return ok && now.Sub(last) < p.ttl
}

// systemOf returns the system prompt, which ParseChat has already constrained to
// at most one message and only in first position.
func systemOf(c Chat) string {
	for _, m := range c.Messages {
		if m.Role == "system" {
			return m.Content
		}
	}
	return ""
}

// promptCacheFor returns a tracker only when the operator asked for one. A nil
// tracker answers false to everything, so the decision site needs no second
// condition and the feature being off cannot be half-on.
func promptCacheFor(c Config) *prefixCache {
	if !c.PromptCaching {
		return nil
	}
	return newPrefixCache()
}
