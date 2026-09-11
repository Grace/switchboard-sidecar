package gateway

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// The safety argument for the whole feature: a fault that means nothing to
// another gateway must never leave this one.
//
// refused is the dangerous case. A 401 is one deployment's wrong key and a 404
// is one deployment's wrong model name; broadcasting either would take a
// provider away from a fleet it is serving perfectly well.
func TestOnlyGloballyMeaningfulFaultsAreShared(t *testing.T) {
	for f, want := range map[fault]bool{
		faultAccount:   true,  // this tenant cannot pay, true for everyone holding the key
		faultDegraded:  true,  // a 5xx, plausibly the provider
		faultRefused:   false, // one deployment's credential or model name
		faultTerminal:  false, // one caller's bad request
		faultRateLimit: false, // this caller's quota, already handled by Retry-After
	} {
		if shareable(f) != want {
			t.Errorf("shareable(%s) = %v, want %v", f, shareable(f), want)
		}
	}
}

func TestNothingIsSharedWhenDisabled(t *testing.T) {
	s := &Server{fleet: nil, reported: map[string]fault{}}
	s.share("openai", faultAccount)
	if len(s.reported) != 0 {
		t.Error("recorded a report with the feature off")
	}
	if s.drainReports() != nil {
		t.Error("drained reports with the feature off")
	}
}

// A fault that has stopped recurring must stop being reported, or one bad
// minute keeps a provider withheld across the fleet indefinitely.
func TestReportsDrainRatherThanAccumulate(t *testing.T) {
	s := &Server{fleet: newFleetHealth(), reported: map[string]fault{}}
	s.share("openai", faultAccount)
	s.share("gemini", faultDegraded)
	s.share("anthropic", faultRefused) // never shared

	first := s.drainReports()
	if len(first) != 2 || first["anthropic"] != 0 {
		t.Fatalf("drained %v, want only openai and gemini", first)
	}
	if again := s.drainReports(); len(again) != 0 {
		t.Errorf("a second drain returned %v; a fault that stopped recurring is still being reported", again)
	}
}

// Hints must lapse faster than they are refreshed, or the control plane going
// away leaves a gateway acting on evidence nobody is renewing.
func TestHintsExpire(t *testing.T) {
	f := newFleetHealth()
	f.apply(map[string]time.Duration{"openai": time.Hour}, fleetHintMax)
	if !f.unhealthy("openai") {
		t.Fatal("a fresh hint was not applied")
	}
	f.mu.Lock()
	for k := range f.seen {
		f.seen[k] = time.Now().Add(-time.Second)
	}
	f.mu.Unlock()
	if f.unhealthy("openai") {
		t.Error("an expired hint is still being acted on")
	}
}

// However long a reporting gateway asked for, this process bounds it. The
// sender is another gateway, not an authority.
func TestHintDurationIsBounded(t *testing.T) {
	f := newFleetHealth()
	f.apply(map[string]time.Duration{"openai": 24 * time.Hour}, fleetHintMax)
	f.mu.Lock()
	until := f.seen["openai"]
	f.mu.Unlock()
	if d := time.Until(until); d > fleetHintMax+time.Second {
		t.Errorf("hint lasts %v, capped at %v", d, fleetHintMax)
	}
}

// The response is the fleet's current view, so a provider absent from it is one
// nobody is reporting. Keeping an old hint would be inventing evidence.
func TestApplyReplacesRatherThanMerges(t *testing.T) {
	f := newFleetHealth()
	f.apply(map[string]time.Duration{"openai": time.Minute, "gemini": time.Minute}, fleetHintMax)
	f.apply(map[string]time.Duration{"gemini": time.Minute}, fleetHintMax)
	if f.unhealthy("openai") {
		t.Error("a provider nobody is reporting any more is still hinted against")
	}
	if !f.unhealthy("gemini") {
		t.Error("a provider still being reported was dropped")
	}
}

func TestNilStoreIsInert(t *testing.T) {
	var f *fleetHealth
	f.apply(map[string]time.Duration{"openai": time.Minute}, fleetHintMax)
	if f.unhealthy("openai") {
		t.Error("a nil store answered true")
	}
	if fleetFor(Config{}) != nil || fleetFor(Config{FleetHealth: true}) == nil {
		t.Error("fleetFor did not follow the config")
	}
}

// The failure this feature could cause, and the rule that prevents it.
//
// If the fleet reports every provider in a policy as unhealthy, acting on all of
// it would leave the gateway with nothing to try -- an outage of its own making,
// while the providers may well be reachable from here. So when every eligible
// route is hinted against, none is. Budget-aware routing already works this way,
// for the same reason, and folding both into one map is what makes the rule
// cover the pair rather than each alone.
func TestFleetHintsCanNeverEmptyThePolicy(t *testing.T) {
	answered := 0
	h := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answered++
		w.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"served anyway"},"finish_reason":"stop"}]}`))
	}))
	defer h.Close()

	s := testServer(t, map[string]ProviderConfig{
		"openai":    {URL: h.URL, KeyEnv: "PROVIDER_KEY"},
		"anthropic": {URL: h.URL, KeyEnv: "PROVIDER_KEY"},
	})
	s.fleet = newFleetHealth()
	// Everything this policy could route to, reported unhealthy at once.
	s.fleet.apply(map[string]time.Duration{
		"openai": time.Minute, "anthropic": time.Minute,
		"gemini": time.Minute, "bedrock": time.Minute,
	}, fleetHintMax)

	w := call(s, chat)
	if w.Code != 200 {
		t.Fatalf("status %d: the fleet emptied the policy and the caller paid for it\n%s", w.Code, w.Body)
	}
	if answered == 0 {
		t.Error("no provider was tried at all")
	}
}

// With one route still clear, the hint does its job.
func TestFleetHintSkipsAReportedProvider(t *testing.T) {
	var first, second atomic.Int64
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first.Add(1)
		w.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"first"},"finish_reason":"stop"}]}`))
	}))
	defer a.Close()
	b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		w.Write([]byte(`{"content":[{"type":"text","text":"second"}],"stop_reason":"end_turn"}`))
	}))
	defer b.Close()

	s := testServer(t, map[string]ProviderConfig{
		"openai":    {URL: a.URL, KeyEnv: "PROVIDER_KEY"},
		"anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"},
	})
	s.fleet = newFleetHealth()
	s.fleet.apply(map[string]time.Duration{"openai": time.Minute}, fleetHintMax)

	if w := call(s, chat); w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	if first.Load() != 0 {
		t.Errorf("the reported provider was tried %d times; the hint did nothing", first.Load())
	}
	if second.Load() != 1 {
		t.Errorf("the clear provider was called %d times, want 1", second.Load())
	}
}
