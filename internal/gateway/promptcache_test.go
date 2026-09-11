package gateway

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func longSystem(seed string) string { return seed + strings.Repeat("x", promptCacheMinBytes) }

// The whole design in one assertion: nothing is marked on first sight.
//
// Anthropic bills a cache write at roughly 1.25x input and a read at roughly
// 0.1x, so marking everything is a 25% surcharge on any prefix that is never
// reused. Marking only on the second sighting means the first request pays
// nothing extra and the wager is placed only where there is evidence for it.
func TestFirstSightingIsNotMarked(t *testing.T) {
	p := newPrefixCache()
	sys := longSystem("a")
	if p.worthCaching(sys) {
		t.Error("marked a system prompt the gateway had never seen; that is a cache write bought on a guess")
	}
	if !p.worthCaching(sys) {
		t.Error("did not mark the second sighting, which is the only one worth paying for")
	}
	if !p.worthCaching(sys) {
		t.Error("stopped marking a prefix that is still repeating")
	}
}

// A prompt short enough that Anthropic would ignore the breakpoint is not worth
// the structured request, and never counts as a sighting either.
func TestShortPromptsAreNeverMarked(t *testing.T) {
	p := newPrefixCache()
	short := strings.Repeat("y", promptCacheMinBytes-1)
	for i := 0; i < 3; i++ {
		if p.worthCaching(short) {
			t.Fatalf("marked a %d-byte prompt on sighting %d; below the minimum cacheable length "+
				"the breakpoint does nothing", len(short), i+1)
		}
	}
}

// Two callers with different system prompts must not make each other's look
// repeated.
func TestDistinctPromptsDoNotCountAsRepeats(t *testing.T) {
	p := newPrefixCache()
	if p.worthCaching(longSystem("a")) || p.worthCaching(longSystem("b")) {
		t.Fatal("a first sighting was marked")
	}
	if !p.worthCaching(longSystem("a")) {
		t.Error("the repeat of the first prompt was not recognised")
	}
}

// A sighting older than the provider's cache lifetime is not evidence: the cache
// it would have written has already expired, so marking buys a write and no
// reads -- the surcharge this exists to avoid.
func TestSightingsExpireWithTheProviderCache(t *testing.T) {
	p := newPrefixCache()
	sys := longSystem("a")
	p.worthCaching(sys)

	p.mu.Lock()
	for k := range p.seen {
		p.seen[k] = time.Now().Add(-promptCacheTTL - time.Second)
	}
	p.mu.Unlock()

	if p.worthCaching(sys) {
		t.Error("marked on a sighting older than the cache lifetime; that write can never be read")
	}
}

// System prompts are caller-supplied, so the key space is unbounded and an
// unbounded map here is a slow memory leak.
func TestTrackingIsBounded(t *testing.T) {
	p := newPrefixCache()
	for i := 0; i < promptCacheMaxTracked+500; i++ {
		p.worthCaching(longSystem(string(rune(i%1000)) + strings.Repeat("z", i%7+1)))
	}
	p.mu.Lock()
	n := len(p.seen)
	p.mu.Unlock()
	if n > promptCacheMaxTracked {
		t.Errorf("tracking %d prompts, cap is %d", n, promptCacheMaxTracked)
	}
}

// Off unless asked for, and a nil tracker answers false rather than panicking,
// so the decision site needs no second condition.
func TestDisabledByDefault(t *testing.T) {
	if promptCacheFor(Config{}) != nil {
		t.Error("prompt caching built a tracker without being configured")
	}
	var off *prefixCache
	if off.worthCaching(longSystem("a")) {
		t.Error("a nil tracker marked a prompt")
	}
	if promptCacheFor(Config{PromptCaching: true}) == nil {
		t.Error("configured and still nil")
	}
}

// The request Anthropic actually receives. A bare string when unmarked, because
// the structured form exists only to hang cache_control off.
func TestAnthropicRequestShape(t *testing.T) {
	sys := longSystem("a")
	c := Chat{Model: "claude-haiku-4-5", MaxTokens: 64, Messages: []Message{
		{Role: "system", Content: sys}, {Role: "user", Content: "hi"}}}
	route := Route{Provider: "anthropic", Model: "claude-haiku-4-5"}
	pc := ProviderConfig{URL: "https://example.invalid", KeyEnv: "PROVIDER_KEY"}
	t.Setenv("PROVIDER_KEY", "k")

	for _, marked := range []bool{false, true} {
		c.CacheSystem = marked
		req, err := upstream(t.Context(), c, route, pc, nil)
		if err != nil {
			t.Fatalf("upstream: %v", err)
		}
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		switch v := body["system"].(type) {
		case string:
			if marked {
				t.Error("marked request sent a bare system string, which cannot carry cache_control")
			}
		case []any:
			if !marked {
				t.Error("unmarked request sent the structured system form for no reason")
				break
			}
			block := v[0].(map[string]any)
			if block["text"] != sys {
				t.Error("the system prompt did not survive the structured form")
			}
			cc, ok := block["cache_control"].(map[string]any)
			if !ok || cc["type"] != "ephemeral" {
				t.Errorf("cache_control = %v, want {type: ephemeral}", block["cache_control"])
			}
		default:
			t.Fatalf("system is %T", v)
		}
	}
}

// A caller must not be able to ask for this. If they could, every one-shot
// prompt could be made to pay a cache write.
func TestCallersCannotRequestCaching(t *testing.T) {
	var c Chat
	if err := json.Unmarshal([]byte(`{"model":"m","cache_system":true,"CacheSystem":true}`), &c); err == nil && c.CacheSystem {
		t.Error("a request body set CacheSystem")
	}
}
