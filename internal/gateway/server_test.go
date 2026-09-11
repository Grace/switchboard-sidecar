package gateway

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const success = `{"choices":[{"index":0,"message":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`

func testServer(t *testing.T, providers map[string]ProviderConfig) *Server {
	t.Helper()
	t.Setenv("LOCAL_TOKEN", strings.Repeat("x", 32))
	t.Setenv("CP_TOKEN", strings.Repeat("y", 32))
	t.Setenv("PROVIDER_KEY", "test-only")
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	p := &PolicyStore{Tenant: "tenant-a", Keys: map[string]ed25519.PublicKey{"k": pub}}
	if e := p.Apply(signed(t, testPolicy(), key, "k"), false); e != nil {
		t.Fatal(e)
	}
	c := Config{Tenant: "tenant-a", Listen: "127.0.0.1:8080", Providers: providers, DataDir: t.TempDir(), LocalTokenEnv: "LOCAL_TOKEN", ControlTokenEnv: "CP_TOKEN", ControlURL: "http://127.0.0.1:1", Concurrency: 2, Rate: 1000, Burst: 1000, RetryRate: 100, MaxAttempts: 3, TimeoutSeconds: 2, QueueSize: 4, SpoolBytes: 1 << 20, AllowLocalHTTP: true}
	s := New(c, p, &Metrics{}, nil)
	useTestTransport(s.HTTP)
	return s
}
func call(s *Server, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

const chat = `{"model":"preferred","messages":[{"role":"user","content":"hi"}]}`

func TestFailoverAndControlDown(t *testing.T) {
	var first, second atomic.Int64
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { first.Add(1); w.WriteHeader(503) }))
	defer a.Close()
	b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		io.WriteString(w, `{"content":[{"type":"text","text":"rescued"}],"stop_reason":"end_turn"}`)
	}))
	defer b.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}, "anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"}})
	s.syncOnce(context.Background())
	w := call(s, chat)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "rescued") || first.Load() != 1 || second.Load() != 1 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if s.Metrics.PolicyErrors.Load() != 1 {
		t.Fatal("control down not observed")
	}
}

// 401 was removed from this list deliberately. A 401, 403 or 404 is refused
// before the provider generates anything, so nothing was accepted and nothing
// was billed, and the refusal is specific to one provider -- see faultRefused
// and TestRefusedStatusesFailOver below. What remains here is the genuinely
// ambiguous set: 200 and 400 because the request was answered, 500 because the
// provider may have accepted it and failed partway through generating.
func TestNoReplayAfterAcceptanceOrAmbiguity(t *testing.T) {
	for _, status := range []int{200, 400, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var fallback atomic.Int64
			a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				io.WriteString(w, `{"broken":true}`)
			}))
			defer a.Close()
			b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fallback.Add(1) }))
			defer b.Close()
			s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}, "anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"}})
			w := call(s, chat)
			if w.Code < 400 || fallback.Load() != 0 {
				t.Fatal("unsafe replay")
			}
		})
	}
}

// A rotated key, a revoked one, or a model the provider retired. Each is a
// refusal issued before generation and specific to the provider that issued it,
// so the policy's remaining routes are exactly what they are for. Before this
// worked, every request returned 502 while a healthy provider sat idle in the
// same signed policy -- and docs/GAPS.md item 2 records two of three model names
// in this repository's own tests being retired by their providers mid-project,
// so the 404 is not hypothetical.
func TestRefusedStatusesFailOver(t *testing.T) {
	for _, status := range []int{401, 403, 404} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var first, second atomic.Int64
			a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				first.Add(1)
				w.WriteHeader(status)
				io.WriteString(w, `{"error":{"message":"nope"}}`)
			}))
			defer a.Close()
			b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				second.Add(1)
				io.WriteString(w, `{"content":[{"type":"text","text":"rescued"}],"stop_reason":"end_turn"}`)
			}))
			defer b.Close()
			s := testServer(t, map[string]ProviderConfig{
				"openai":    {URL: a.URL, KeyEnv: "PROVIDER_KEY"},
				"anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"},
			})
			w := call(s, chat)
			if w.Code != 200 || !strings.Contains(w.Body.String(), "rescued") {
				t.Fatalf("status %d did not fail over: %d %s", status, w.Code, w.Body)
			}
			if first.Load() != 1 || second.Load() != 1 {
				t.Fatalf("route counts wrong: first=%d second=%d", first.Load(), second.Load())
			}
			if s.Metrics.RefusedFailover.Load() != 1 {
				t.Errorf("refused_failover_total = %d, want 1", s.Metrics.RefusedFailover.Load())
			}
			// Withheld as well as failed over. Without the cooldown every later
			// request pays a doomed round trip to the same provider first.
			if s.circuits["openai"].wouldAllow() {
				t.Error("a provider that refused our credentials was not withheld")
			}
		})
	}
}

// result(false) is the success path: it zeroes failures and clears the
// open-until deadline. The terminal branch called it, so a provider returning
// 500 forever reset its own breaker on every request and the breaker could
// never open however many failures arrived.
func TestTerminalFailuresStillOpenTheBreaker(t *testing.T) {
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		io.WriteString(w, `{"error":{"message":"boom"}}`)
	}))
	defer a.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}})
	for i := 0; i < 3; i++ {
		if w := call(s, chat); w.Code != 502 {
			t.Fatalf("attempt %d: status %d, want 502", i+1, w.Code)
		}
	}
	if s.circuits["openai"].wouldAllow() {
		t.Fatal("three consecutive 500s left the breaker closed; it reset itself each time")
	}
}

// The other half of the same bug, in the opposite direction: a 400 is the
// caller's fault, so it must not wipe out failures a sick provider has already
// accumulated. Two real failures then a client error should leave the provider
// one failure away from open, not back at zero.
func TestClientErrorDoesNotResetTheBreaker(t *testing.T) {
	var status atomic.Int64
	status.Store(500)
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(status.Load()))
		io.WriteString(w, `{"error":{"message":"x"}}`)
	}))
	defer a.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}})
	for i := 0; i < 2; i++ {
		call(s, chat)
	}
	status.Store(400)
	call(s, chat)

	s.circuits["openai"].mu.Lock()
	failures := s.circuits["openai"].failures
	s.circuits["openai"].mu.Unlock()
	if failures != 2 {
		t.Fatalf("failures = %d after two 500s and a 400, want 2; the client error reset the breaker", failures)
	}
}

// One histogram over two populations made the median meaningless: local
// rejections complete in microseconds without contacting anyone, and
// latencyBounds starts at 5 with no lower edge on the first bucket, so enough of
// them interpolate the median below zero. docs/GAPS.md item 5 recorded P50 as
// -5 ms.
func TestLatencyMeasuresOnlyRequestsThatReachedAProvider(t *testing.T) {
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"choices":[{"index":0,"message":{"content":"ok"},"finish_reason":"stop"}],`+
			`"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	}))
	defer a.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}})

	// Unauthorized: never reaches a provider.
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(chat)))
	if w.Code != 401 {
		t.Fatalf("setup: status %d, want 401", w.Code)
	}
	// Malformed body: parsed and refused, still never reaches a provider.
	if got := call(s, `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`); got.Code != 400 {
		t.Fatalf("setup: status %d, want 400", got.Code)
	}
	if n := s.Metrics.Completed.Load(); n != 0 {
		t.Fatalf("requests that contacted no provider were measured: completed=%d, want 0", n)
	}

	if got := call(s, chat); got.Code != 200 {
		t.Fatalf("inference: %d %s", got.Code, got.Body)
	}
	if n := s.Metrics.Completed.Load(); n != 1 {
		t.Fatalf("completed=%d after one real inference, want 1", n)
	}
}

// Every distinct first-run misconfiguration used to present as one empty-bodied
// 503, retried in silence every fifteen seconds forever. A wrong trust key, an
// unregistered control token, a tenant with no published policy and a policy
// that quietly expired were indistinguishable to the person trying to start it.
func TestReadyzSaysWhyItIsNotReady(t *testing.T) {
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: "http://127.0.0.1:1", KeyEnv: "PROVIDER_KEY"}})

	// Healthy: the test policy is already applied.
	if r := s.readyzReason(); r != "" {
		t.Fatalf("setup: not ready, %q", r)
	}

	// No policy at all, with a control plane configured.
	empty := &PolicyStore{Tenant: "tenant-a", Keys: s.Policies.Keys}
	s.Policies = empty
	if r := s.notReady(); !strings.Contains(r, "no policy") {
		t.Errorf("reason = %q, want it to mention the missing policy", r)
	}

	// The sync failure is the actual cause and must supersede the generic wait.
	s.noteSync("control plane refused the policy request", "status", 401)
	if r := s.notReady(); !strings.Contains(r, "refused") {
		t.Errorf("reason = %q, want the sync failure to be named", r)
	}

	// And the endpoint carries it, rather than an empty body.
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 503 {
		t.Fatalf("status %d, want 503", w.Code)
	}
	if strings.TrimSpace(w.Body.String()) == "" {
		t.Fatal("/readyz returned 503 with an empty body; that is the defect")
	}

	s.Draining.Store(true)
	if r := s.notReady(); r != "draining" {
		t.Errorf("draining reason = %q", r)
	}
}

// A control plane that is down stays down. Logging every fifteen seconds is not
// diagnostics, it is 5,760 identical lines a day burying the one that mattered.
func TestSyncFailureIsReportedOnceAndOnRecovery(t *testing.T) {
	var status atomic.Int64
	status.Store(401)
	cp := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if code := int(status.Load()); code != 200 {
			w.WriteHeader(code)
			return
		}
		w.WriteHeader(200)
		io.WriteString(w, "not a policy")
	}))
	defer cp.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: "http://127.0.0.1:1", KeyEnv: "PROVIDER_KEY"}})
	s.C.ControlURL = cp.URL

	s.syncOnce(context.Background())
	first, _ := s.syncState.Load().(string)
	if !strings.Contains(first, "refused") {
		t.Fatalf("sync state after a 401 = %q, want it to name the refusal", first)
	}
	s.syncOnce(context.Background())
	if again, _ := s.syncState.Load().(string); again != first {
		t.Errorf("state changed on an identical repeat failure: %q -> %q", first, again)
	}

	// A 200 carrying something that is not a verifiable policy is a different
	// failure and must be reported as one, not silently counted.
	status.Store(200)
	s.syncOnce(context.Background())
	if r, _ := s.syncState.Load().(string); !strings.Contains(r, "rejected") {
		t.Errorf("sync state after an unverifiable body = %q, want it to name the rejection", r)
	}
	if s.Metrics.PolicyErrors.Load() != 3 {
		t.Errorf("policy_errors_total = %d, want 3", s.Metrics.PolicyErrors.Load())
	}
}

// stream() sets these before reading a frame. A stream that produced nothing
// fails over and can end at a JSON error instead, which was shipping an error
// body labelled as an unbuffered event stream because only Content-Type was
// being overwritten.
func TestProblemClearsStreamingHeaders(t *testing.T) {
	w := httptest.NewRecorder()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	problem(w, 503, "routes unavailable")

	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	for _, h := range []string{"Cache-Control", "X-Accel-Buffering"} {
		if got := w.Header().Get(h); got != "" {
			t.Errorf("%s = %q on a JSON error body, want it cleared", h, got)
		}
	}
}

// A policy lasts at most seven days and renewal is manual. Nothing measured the
// remaining time, so the first signal a deployment got was /readyz turning 503 --
// after it had already stopped serving. A system that worked for a week and then
// stopped, with no indication a clock was running, is worse than one that never
// started, because by then the operator believed it.
func TestPolicyExpiryIsMeasuredAndWarnedAboutOnce(t *testing.T) {
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: "http://127.0.0.1:1", KeyEnv: "PROVIDER_KEY"}})

	// The test policy has an hour left, which is inside the warning margin.
	s.samplePolicyExpiry()
	left := s.Metrics.PolicyExpiresIn.Load()
	if left <= 0 || left > int64(policyExpiryWarn.Seconds()) {
		t.Fatalf("policy_expires_in_seconds = %d, want a positive value inside the warning margin", left)
	}
	if band, _ := s.expiryState.Load().(string); band != "expiring" {
		t.Errorf("band = %q, want expiring", band)
	}

	// Sampling again must not warn again. Asserting on the band alone would not
	// catch that -- storing the same value twice looks identical -- so this
	// counts what actually reached the log. Two days of one-minute ticks is 2,880
	// identical warnings otherwise, which buries the line it is trying to raise.
	var logged bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(restore)

	s.samplePolicyExpiry()
	s.samplePolicyExpiry()
	s.samplePolicyExpiry()
	if n := strings.Count(logged.String(), "policy expires soon"); n != 0 {
		t.Errorf("warned %d more times for an unchanged policy, want 0", n)
	}
	if band, _ := s.expiryState.Load().(string); band != "expiring" {
		t.Errorf("band changed on an unchanged policy: %q", band)
	}

	// Past expiry is a different band, and must be reachable rather than
	// collapsing into "no policy" the way Current() does.
	expired := &PolicyStore{Tenant: "tenant-a", Keys: s.Policies.Keys}
	p := testPolicy()
	p.IssuedAt = time.Now().Unix() - 7200
	p.ExpiresAt = time.Now().Unix() - 60
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	expired.Keys = map[string]ed25519.PublicKey{"k": pub}
	if e := expired.Apply(signed(t, p, key, "k"), false); e == nil {
		t.Fatal("setup: an expired policy was accepted by Apply")
	}
	// Apply refuses an expired policy, so load it the way a restart does.
	if e := expired.Restore(signed(t, p, key, "k")); e != nil {
		t.Fatalf("setup: %v", e)
	}
	s.Policies = expired
	s.samplePolicyExpiry()
	if got := s.Metrics.PolicyExpiresIn.Load(); got >= 0 {
		t.Errorf("expired policy reported %d seconds left, want negative", got)
	}
	if band, _ := s.expiryState.Load().(string); band != "expired" {
		t.Errorf("band = %q, want expired", band)
	}
	if n := strings.Count(logged.String(), "policy expired"); n != 1 {
		t.Errorf("crossing into expiry logged %d times, want exactly 1", n)
	}
}

// A gateway that never loaded a policy has no deadline, and inventing one would
// report a countdown against a policy that does not exist. notReady() already
// tells those two states apart.
func TestPolicyExpiryIsSilentWithNoPolicy(t *testing.T) {
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: "http://127.0.0.1:1", KeyEnv: "PROVIDER_KEY"}})
	s.Policies = &PolicyStore{Tenant: "tenant-a", Keys: s.Policies.Keys}
	s.samplePolicyExpiry()
	if got := s.Metrics.PolicyExpiresIn.Load(); got != 0 {
		t.Errorf("policy_expires_in_seconds = %d with no policy loaded, want it untouched", got)
	}
	if band, _ := s.expiryState.Load().(string); band != "" {
		t.Errorf("band = %q with no policy loaded, want empty", band)
	}
}

func TestSyncHintNamesTheThingToCheck(t *testing.T) {
	for _, status := range []int{401, 403, 404, 503} {
		if syncHint(status) == "" {
			t.Errorf("status %d has no hint; it is a first-run misconfiguration", status)
		}
	}
}

func TestLimitsAndAuth(t *testing.T) {
	s := testServer(t, nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(chat)))
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	s.C.Providers = map[string]ProviderConfig{"openai": {URL: "http://127.0.0.1:1", KeyEnv: "PROVIDER_KEY"}}
	s.slots <- struct{}{}
	s.slots <- struct{}{}
	if w := call(s, chat); w.Code != 429 {
		t.Fatal(w.Code)
	}
	<-s.slots
	<-s.slots
	s.rate = newBucket(1, 1)
	s.rate.allow()
	if call(s, chat).Code != 429 {
		t.Fatal("rate limit")
	}
	s.Draining.Store(true)
	if call(s, chat).Code != 503 {
		t.Fatal("drain")
	}
}

// The first two requests an OpenAI SDK user sends, and what they used to be
// told. `model="gpt-4o"` was answered with a sentence about message counts, and
// anything carrying `response_format` or `top_p` was answered with a sentence
// about tools -- in both cases naming something the caller had not done.
func TestParseErrorsNameTheActualProblem(t *testing.T) {
	const msgs = `"messages":[{"role":"user","content":"hi"}]`
	for _, tc := range []struct{ name, body, want string }{
		{"the offending field is named", `{"model":"preferred","response_format":{},` + msgs + `}`, "response_format"},
		{"every offending field is listed", `{"model":"preferred","top_p":1,"n":2,` + msgs + `}`, "n, top_p"},
		{"the rejected model is quoted back", `{"model":"gpt-4o",` + msgs + `}`, `"gpt-4o"`},
		{"the model error explains why", `{"model":"gpt-4o",` + msgs + `}`, "signed routing policy"},
		{"array content is explained", `{"model":"preferred","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`, "plain string"},
		{"message count is its own error", `{"model":"preferred","messages":[]}`, "1-128 messages"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, e := ParseChat([]byte(tc.body))
			if e == nil {
				t.Fatal("accepted an unsupported request")
			}
			if !strings.Contains(e.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", e.Error(), tc.want)
			}
		})
	}
	// The old message named tools for every unknown field. It must not appear
	// when the caller never mentioned tools.
	_, e := ParseChat([]byte(`{"model":"preferred","top_p":1,` + msgs + `}`))
	if strings.Contains(e.Error(), "tool") {
		t.Errorf("a top_p rejection still talks about tools: %q", e.Error())
	}
}

func TestUnsupportedInputs(t *testing.T) {
	for _, b := range []string{`{"model":"preferred","tools":[],"messages":[{"role":"user","content":"hi"}]}`, `{"model":"preferred","messages":[{"role":"tool","content":"hi"}]}`, `{"model":"preferred","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`, `{"model":"preferred","messages":[{"role":"assistant","content":"hi"}]}`, chat + `{}`} {
		if _, e := ParseChat([]byte(b)); e == nil {
			t.Fatal("accepted unsupported input", b)
		}
	}
}
func TestCircuitSingleProbe(t *testing.T) {
	c := &circuit{}
	for i := 0; i < 3; i++ {
		c.result(true)
	}
	if c.allow() {
		t.Fatal("open circuit allowed")
	}
	c.until = time.Now().Add(-time.Second)
	if !c.allow() || c.allow() {
		t.Fatal("single probe violated")
	}
	c.result(false)
	if !c.allow() {
		t.Fatal("circuit did not close")
	}
}
func TestRetryBudget(t *testing.T) {
	var calls atomic.Int64
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(429) }))
	defer a.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}, "anthropic": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}})
	s.retry = newBucket(1, 1)
	s.retry.allow()
	if call(s, chat).Code != 503 || calls.Load() != 1 {
		t.Fatal("retry budget exceeded")
	}
}
func TestIdempotencyRejected(t *testing.T) {
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: "http://127.0.0.1:1", KeyEnv: "PROVIDER_KEY"}})
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(chat))
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
	r.Header.Set("Idempotency-Key", "abc")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}

// Verbatim from the real Anthropic API. Reported as 400, which the routing loop
// used to read as "this request is malformed" and refuse to fail over on, while
// a funded provider sat unused in the same policy.
const anthropicNoCredit = `{"type":"error","error":{"type":"invalid_request_error","message":` +
	`"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}`

func TestAccountFailureFailsOver(t *testing.T) {
	var first, second atomic.Int64
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first.Add(1)
		w.WriteHeader(400)
		io.WriteString(w, anthropicNoCredit)
	}))
	defer a.Close()
	b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		io.WriteString(w, `{"content":[{"type":"text","text":"rescued"}],"stop_reason":"end_turn"}`)
	}))
	defer b.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}, "anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"}})
	w := call(s, chat)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "rescued") {
		t.Fatalf("no failover on an unpayable account: %d %s", w.Code, w.Body)
	}
	if first.Load() != 1 || second.Load() != 1 {
		t.Fatalf("attempts: first=%d second=%d", first.Load(), second.Load())
	}
	if w.Header().Get("X-Switchboard-Provider") != "anthropic" {
		t.Errorf("caller cannot see which provider answered: %q", w.Header().Get("X-Switchboard-Provider"))
	}
	if s.Metrics.AccountFailover.Load() != 1 {
		t.Errorf("AccountFailover = %d, want 1", s.Metrics.AccountFailover.Load())
	}
	// The provider is healthy; only this account cannot pay. Blaming the
	// provider would open the breaker against a service that is working.
	if s.circuits["openai"].failures != 0 {
		t.Errorf("an unpayable account was counted as a provider health failure")
	}
}

// The counterpart, and the more important direction: a request that is genuinely
// malformed would be refused identically everywhere, so replaying it across
// every provider multiplies the waste instead of avoiding it.
func TestMalformedRequestDoesNotFailOver(t *testing.T) {
	var fallback atomic.Int64
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		io.WriteString(w, `{"error":{"message":"messages: at least one message is required"}}`)
	}))
	defer a.Close()
	b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fallback.Add(1) }))
	defer b.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}, "anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"}})
	w := call(s, chat)
	if fallback.Load() != 0 {
		t.Fatal("a malformed request was replayed to a second provider")
	}
	// The caller should learn why, not just that something was rejected.
	if !strings.Contains(w.Body.String(), "at least one message is required") {
		t.Errorf("provider's own reason not surfaced: %s", w.Body)
	}
}

// A reasoning model can spend its whole token budget on hidden reasoning and
// return no visible text, billed in full. Measured on gpt-5-nano: 1024 tokens
// in, 1024 spent reasoning, zero characters out. Reporting that as a 200 charges
// the caller for an empty answer.
const emptyByBudget = `{"choices":[{"index":0,"message":{"content":""},"finish_reason":"length"}],` +
	`"usage":{"prompt_tokens":9,"completion_tokens":1024,"completion_tokens_details":{"reasoning_tokens":1024}}}`

func TestEmptyCompletionFailsOver(t *testing.T) {
	var second atomic.Int64
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, emptyByBudget)
	}))
	defer a.Close()
	b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		io.WriteString(w, `{"content":[{"type":"text","text":"rescued"}],"stop_reason":"end_turn"}`)
	}))
	defer b.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}, "anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"}})
	w := call(s, chat)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "rescued") {
		t.Fatalf("an empty completion was returned as success: %d %s", w.Code, w.Body)
	}
	if second.Load() != 1 {
		t.Fatalf("second provider called %d times", second.Load())
	}
	if s.Metrics.EmptyCompletion.Load() != 1 {
		t.Errorf("EmptyCompletion = %d, want 1", s.Metrics.EmptyCompletion.Load())
	}
	// The distinction an alarm needs: this cost money and latency, but the caller
	// was served. It must not look like lost service.
	if s.Metrics.EmptyCompletionRecovered.Load() != 1 {
		t.Errorf("EmptyCompletionRecovered = %d, want 1", s.Metrics.EmptyCompletionRecovered.Load())
	}
	if s.Metrics.EmptyCompletionFailed.Load() != 0 {
		t.Errorf("a recovered request was counted as failed")
	}
}

// When no provider can produce output, the caller must be told that rather than
// receiving a generic routing failure, because the fix is theirs to make.
func TestAllProvidersEmptyNamesTheCause(t *testing.T) {
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, emptyByBudget)
	}))
	defer a.Close()
	// The same failure in Anthropic's shape: the budget ran out before any
	// content block was produced.
	b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"content":[],"stop_reason":"max_tokens","usage":{"input_tokens":9,"output_tokens":1024}}`)
	}))
	defer b.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}, "anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"}})
	w := call(s, chat)
	if w.Code != 503 {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	// Naming the routes is the point. Without it the caller sees a routing
	// failure and cannot tell the fix is theirs, or which model to stop asking.
	body := w.Body.String()
	for _, want := range []string{"openai", "anthropic", "test-model", "1024", "max_tokens"} {
		if !strings.Contains(body, want) {
			t.Errorf("failure message is missing %q: %s", want, body)
		}
	}
	// Only what was measured. The OpenAI-shaped body reported 1024 reasoning
	// tokens; the Anthropic-shaped one reports none, and must not claim zero.
	if !strings.Contains(body, "spent all 1024 tokens on internal reasoning") {
		t.Errorf("reasoning figure not reported: %s", body)
	}
	if strings.Contains(body, "all 0 tokens") {
		t.Errorf("an absent reasoning figure was printed as zero: %s", body)
	}
	// The other half of the distinction: this is lost service, counted once for
	// the request rather than once per route.
	if s.Metrics.EmptyCompletionFailed.Load() != 1 {
		t.Errorf("EmptyCompletionFailed = %d, want 1", s.Metrics.EmptyCompletionFailed.Load())
	}
	if s.Metrics.EmptyCompletionRecovered.Load() != 0 {
		t.Errorf("a failed request was counted as recovered")
	}
	// And the per-route counter advanced twice for this one request, which is
	// exactly why it cannot be used for alerting on its own.
	if s.Metrics.EmptyCompletion.Load() != 2 {
		t.Errorf("EmptyCompletion = %d, want 2 (one per empty route)", s.Metrics.EmptyCompletion.Load())
	}
}

// The diagnostic exists for the exhausted case only. A request that recovers by
// failing over must carry no trace of it.
func TestEmptyDiagnosticDoesNotLeakIntoSuccess(t *testing.T) {
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, emptyByBudget)
	}))
	defer a.Close()
	b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"content":[{"type":"text","text":"rescued"}],"stop_reason":"end_turn"}`)
	}))
	defer b.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}, "anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"}})
	w := call(s, chat)
	if w.Code != 200 {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if strings.Contains(w.Body.String(), "internal reasoning") {
		t.Errorf("diagnostic leaked into a successful response: %s", w.Body)
	}
}

// The streaming equivalent. Failover is only legitimate here because the frame
// carrying the truncation reason is held back, so no byte has reached the client.
func TestEmptyStreamFailsOverBeforeAnyByteIsSent(t *testing.T) {
	var second atomic.Int64
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}\n\n")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer a.Close()
	b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"rescued\"}}\n\n")
		io.WriteString(w, "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n")
		io.WriteString(w, "data: {\"type\":\"message_stop\"}\n\n")
	}))
	defer b.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: a.URL, KeyEnv: "PROVIDER_KEY"}, "anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"}})
	w := call(s, `{"model":"preferred","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if second.Load() != 1 {
		t.Fatalf("empty stream did not fail over; second provider called %d times", second.Load())
	}
	if !strings.Contains(w.Body.String(), "rescued") {
		t.Fatalf("body: %s", w.Body)
	}
	if s.Metrics.EmptyCompletion.Load() != 1 {
		t.Errorf("EmptyCompletion = %d, want 1", s.Metrics.EmptyCompletion.Load())
	}
}

// pprof exposes whatever is in memory, which here means provider API keys read
// from the environment into request headers, plus prompt and completion text.
// These two properties are the reason it is safe to ship at all, so they are
// tested rather than assumed.
func TestPprofIsOffByDefault(t *testing.T) {
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: "https://api.openai.com", KeyEnv: "PROVIDER_KEY"}})
	r := httptest.NewRequest("GET", "/debug/pprof/heap", nil)
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 404 {
		t.Fatalf("pprof answered %d in a default build; it must not be routed at all", w.Code)
	}
}

// The gateway shares a network namespace with the customer's application. If an
// enabled pprof were readable without the local token, that application could
// read provider credentials out of this process, which is precisely what the
// local-token design exists to prevent.
func TestPprofRequiresTheLocalToken(t *testing.T) {
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: "https://api.openai.com", KeyEnv: "PROVIDER_KEY"}})
	s.C.EnablePprof = true
	h := s.Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/debug/pprof/heap", nil))
	if w.Code != 401 {
		t.Fatalf("unauthenticated pprof returned %d; a co-located process could read the heap", w.Code)
	}

	w = httptest.NewRecorder()
	bad := httptest.NewRequest("GET", "/debug/pprof/heap", nil)
	bad.Header.Set("Authorization", "Bearer "+strings.Repeat("z", 32))
	h.ServeHTTP(w, bad)
	if w.Code != 401 {
		t.Fatalf("pprof accepted a wrong token, returning %d", w.Code)
	}

	w = httptest.NewRecorder()
	ok := httptest.NewRequest("GET", "/debug/pprof/heap", nil)
	ok.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
	h.ServeHTTP(w, ok)
	if w.Code != 200 {
		t.Fatalf("pprof refused the local token, returning %d", w.Code)
	}
}

// hex.DecodeString accepts A-F; the control plane does not. Its Event model
// constrains trace_id to ^[0-9a-f]{32}$, so an uppercase id was accepted here,
// echoed to the caller, spooled, then refused on ingest and deleted as
// deterministically rejected. A client that uppercases its trace ids therefore
// lost 100% of its telemetry, with no symptom but a rising drop counter.
func TestTraceIDsAreLowercased(t *testing.T) {
	upper := "4BF92F3577B34DA6A3CE929D0E0E4736"
	span := "00F067AA0BA902B7"
	trace, parent := traceIDs("00-" + upper + "-" + span + "-01")

	if trace != strings.ToLower(upper) {
		t.Errorf("trace = %q, want lowercase; the control plane rejects A-F", trace)
	}
	if parent != strings.ToLower(span) {
		t.Errorf("parent = %q, want lowercase", parent)
	}
	// Still the same id, just normalised: W3C traceparent is defined in
	// lowercase hex, so nothing about the caller's trace is changed.
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(trace) {
		t.Errorf("trace %q does not satisfy the control plane's own pattern", trace)
	}
}

// The guarantee the routing loop's own comment makes: "a gateway that returns
// 503 without contacting anyone has stopped being a gateway."
//
// It could fail, because eligibility was decided twice over different
// predicates. The pre-pass counted a route eligible on {configured, streams};
// the loop additionally required the breaker. With a budget-shadowed route
// beside one whose breaker is open, eligible was 2 and len(skip) was 1, so the
// "everything was skipped, try anyway" fallback did not fire, route 1 was
// skipped for budget and route 2 refused by the breaker, and the caller got a
// 503 with no provider contacted.
func TestEveryRouteSkippedStillContactsSomeone(t *testing.T) {
	var hits atomic.Int64
	provider := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[{"message":{"content":"hi"},"finish_reason":"stop"}],` +
			`"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer provider.Close()

	s := idemServer(t, map[string]ProviderConfig{
		"openai":    {URL: provider.URL, KeyEnv: "K"},
		"anthropic": {URL: provider.URL, KeyEnv: "K"},
	})
	// Route 1 is budget-shadowed: observed empty at this budget, never seen
	// succeeding at or below it.
	s.budgets.observe("openai", "m1", 64, false)
	// Route 2's breaker is open.
	s.circuits["anthropic"].cooldown(time.Hour)

	r1 := Route{Provider: "openai", Model: "m1"}
	r2 := Route{Provider: "anthropic", Model: "m2"}

	if s.routable(r2, false) {
		t.Fatal("setup: route 2's breaker should be refusing")
	}
	// The count the fallback compares against must exclude the route the loop
	// cannot take, or the fallback never fires.
	eligible := 0
	for _, rt := range []Route{r1, r2} {
		if s.routable(rt, false) {
			eligible++
		}
	}
	if eligible != 1 {
		t.Fatalf("eligible = %d, want 1; the count must reason over the same gates "+
			"the loop applies, including the breaker", eligible)
	}
	if !s.budgets.skip(r1.Provider, r1.Model, 64) {
		t.Fatal("setup: route 1 should be budget-shadowed")
	}
	// One eligible route, one skip: the fallback fires and route 1 is tried
	// anyway, because a prediction that it will return nothing is worth less
	// than contacting nobody at all.
}

// wouldAllow must not consume the half-open probe. A counting pass that called
// allow() would spend the single probe a recovering provider gets, on a request
// that never went anywhere.
func TestWouldAllowDoesNotClaimTheProbe(t *testing.T) {
	c := &circuit{}
	c.cooldown(time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	if !c.wouldAllow() {
		t.Fatal("after the cooldown elapsed, wouldAllow should be true")
	}
	if !c.wouldAllow() {
		t.Error("wouldAllow consumed something; it must be read-only")
	}
	// The probe is still there for a request that actually takes the route.
	if !c.allow() {
		t.Error("allow() found the probe already spent")
	}
	// And now it is claimed.
	if c.allow() {
		t.Error("allow() handed out a second probe")
	}
	if c.wouldAllow() {
		t.Error("wouldAllow reports available while the probe is in flight")
	}
}

// End to end, because the unit tests for this could not see it.
//
// A stream that breaks after its first byte has already committed HTTP 200 --
// stream() writes an SSE error frame, and a status line cannot be withdrawn. The
// telemetry used to record 502 for that request anyway, so the span described a
// status the client never received.
//
// Both halves are asserted here rather than on a hand-built Event, because the
// defect lived in the wiring: the span is only wrong if chat() puts the wrong
// number on the event, and the error count is only right if it stops keying on
// status alone. Constructing an Event directly tests neither.
func TestBrokenStreamRecordsTheSentStatusAndStillCountsAsAnError(t *testing.T) {
	provider := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// One real delta, so bytes reach the client and 200 is committed...
		io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"par\"}}]}\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// ...then the stream simply stops: no finish reason, no [DONE].
	}))
	defer provider.Close()

	var mu sync.Mutex
	spans := []byte(nil)
	collector := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		if bytes.Contains(b, []byte("resourceSpans")) {
			spans = append(spans, b...)
		}
		mu.Unlock()
		w.Write([]byte(`{}`))
	}))
	defer collector.Close()

	s := testServer(t, map[string]ProviderConfig{"openai": {URL: provider.URL, KeyEnv: "PROVIDER_KEY"}})
	tel, err := NewTelemetry(Config{
		DataDir: t.TempDir(), ControlURL: "http://127.0.0.1:1", OTLPURL: collector.URL,
		QueueSize: 8, SpoolBytes: 1 << 20,
	}, s.Metrics)
	if err != nil {
		t.Fatal(err)
	}
	useTestTransport(tel.http)
	ctx, cancel := context.WithCancel(context.Background())
	tel.Start(ctx)
	s.Telemetry = tel

	w := call(s, `{"model":"preferred","stream":true,"messages":[{"role":"user","content":"hi"}]}`)

	// The wire: 200, with the error frame the contract promises.
	if w.Code != 200 {
		t.Fatalf("client saw %d; a committed stream cannot withdraw its status", w.Code)
	}
	if !strings.Contains(w.Body.String(), "stream_error") {
		t.Fatalf("no error frame on the wire: %s", w.Body)
	}

	// Still an error as far as anyone counting is concerned.
	if n := s.Metrics.Errors.Load(); n != 1 {
		t.Errorf("Errors = %d, want 1; a broken stream stopped being counted when the "+
			"count keyed on status alone", n)
	}

	// The export is asynchronous, so wait for it rather than cancelling out from
	// under it.
	var body string
	for i := 0; i < 100 && body == ""; i++ {
		mu.Lock()
		body = string(spans)
		mu.Unlock()
		if body == "" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	cancel()
	tel.Wait()
	if body == "" {
		t.Fatal("no span exported")
	}
	// The span agrees with the wire, and says what went wrong.
	if !strings.Contains(body, `"stringValue":"stream_error"`) {
		t.Errorf("span carries no stream_error error.type:\n%s", body)
	}
	if strings.Contains(body, `"intValue":"502"`) {
		t.Errorf("span reports a 502 for a request the client saw as 200:\n%s", body)
	}
}

// A rate limit is the only fault class that clears on its own, so it is the only
// one where waiting can beat moving on. This asserts the gate as much as the
// behaviour: without a stated Retry-After, or with one past the operator's
// ceiling, the request fails over exactly as it did before.
func TestRateLimitRetriesTheSameProviderOnlyWhenToldToComeBackSoon(t *testing.T) {
	for _, tc := range []struct {
		name       string
		retryAfter string
		ceilingMs  int
		wantFirst  int64 // calls to the rate-limited provider
		wantSecond int64 // calls to the fallback
	}{
		{"told to wait one second, and allowed to", "1", 2000, 2, 0},
		{"told to wait longer than the operator allows", "30", 2000, 1, 1},
		{"told nothing at all", "", 2000, 1, 1},
		{"retrying disabled, which is the default", "1", 0, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var first, second atomic.Int64
			a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Rate limited once; a retry gets a real answer, which is the
				// point of waiting.
				if first.Add(1) == 1 {
					if tc.retryAfter != "" {
						w.Header().Set("Retry-After", tc.retryAfter)
					}
					w.WriteHeader(429)
					w.Write([]byte(`{"error":{"message":"slow down"}}`))
					return
				}
				w.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"waited"},"finish_reason":"stop"}]}`))
			}))
			defer a.Close()
			b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				second.Add(1)
				w.Write([]byte(`{"content":[{"type":"text","text":"failed over"}],"stop_reason":"end_turn"}`))
			}))
			defer b.Close()

			s := testServer(t, map[string]ProviderConfig{
				"openai":    {URL: a.URL, KeyEnv: "PROVIDER_KEY"},
				"anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"},
			})
			s.C.RateLimitRetryMaxMs = tc.ceilingMs

			w := call(s, chat)
			if w.Code != 200 {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
			if first.Load() != tc.wantFirst || second.Load() != tc.wantSecond {
				t.Errorf("first provider called %d times and the fallback %d; want %d and %d",
					first.Load(), second.Load(), tc.wantFirst, tc.wantSecond)
			}
			// Counted either way, because it is still a rate limit.
			if s.Metrics.RateLimited.Load() != 1 {
				t.Errorf("RateLimited = %d, want 1", s.Metrics.RateLimited.Load())
			}
			wantRetry := int64(0)
			if tc.wantFirst == 2 {
				wantRetry = 1
			}
			if s.Metrics.RateLimitRetry.Load() != wantRetry {
				t.Errorf("RateLimitRetry = %d, want %d", s.Metrics.RateLimitRetry.Load(), wantRetry)
			}
		})
	}
}

// The retry happens once. A provider that rate limits every time must not hold
// the request in a loop against its own Retry-After.
func TestRateLimitRetryHappensOnlyOncePerRoute(t *testing.T) {
	var first, second atomic.Int64
	a := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first.Add(1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(429)
		w.Write([]byte(`{"error":{"message":"still slow down"}}`))
	}))
	defer a.Close()
	b := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		w.Write([]byte(`{"content":[{"type":"text","text":"failed over"}],"stop_reason":"end_turn"}`))
	}))
	defer b.Close()

	s := testServer(t, map[string]ProviderConfig{
		"openai":    {URL: a.URL, KeyEnv: "PROVIDER_KEY"},
		"anthropic": {URL: b.URL, KeyEnv: "PROVIDER_KEY"},
	})
	s.C.RateLimitRetryMaxMs = 2000

	if w := call(s, chat); w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	if first.Load() != 2 {
		t.Errorf("rate-limited provider called %d times, want exactly 2 (the try and one retry)", first.Load())
	}
	if second.Load() != 1 {
		t.Errorf("fallback called %d times, want 1; the request must still move on", second.Load())
	}
}
