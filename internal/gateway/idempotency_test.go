package gateway

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func idemServer(t *testing.T, providers map[string]ProviderConfig) *Server {
	t.Helper()
	s := testServer(t, providers)
	store, err := NewIdemStore(t.TempDir(), time.Minute, 1<<20, s.Metrics)
	if err != nil {
		t.Fatal(err)
	}
	s.Idem = store
	return s
}

func callKeyed(s *Server, body, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

// The entire point. A retried key must not reach the provider again, because
// the second call is the one that charges twice.
func TestDuplicateKeyDoesNotCallTheProviderAgain(t *testing.T) {
	var calls atomic.Int64
	h := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.WriteString(w, success)
	}))
	defer h.Close()
	s := idemServer(t, map[string]ProviderConfig{"openai": {URL: h.URL, KeyEnv: "PROVIDER_KEY"}})

	first := callKeyed(s, chat, "key-1")
	if first.Code != 200 {
		t.Fatalf("first request: %d %s", first.Code, first.Body)
	}
	second := callKeyed(s, chat, "key-1")
	if second.Code != 200 {
		t.Fatalf("replay: %d %s", second.Code, second.Body)
	}
	if calls.Load() != 1 {
		t.Fatalf("provider called %d times; the duplicate was charged", calls.Load())
	}
	if first.Body.String() != second.Body.String() {
		t.Error("replay returned a different body than the original")
	}
	if second.Header().Get("X-Switchboard-Replayed") != "true" {
		t.Error("a replay is indistinguishable from a fresh generation")
	}
	if s.Metrics.IdempotentReplay.Load() != 1 {
		t.Errorf("IdempotentReplay = %d, want 1", s.Metrics.IdempotentReplay.Load())
	}
}

// Answering a reused key with the first response would be silently wrong.
func TestSameKeyDifferentBodyIsRefused(t *testing.T) {
	h := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, success)
	}))
	defer h.Close()
	s := idemServer(t, map[string]ProviderConfig{"openai": {URL: h.URL, KeyEnv: "PROVIDER_KEY"}})
	callKeyed(s, chat, "key-2")
	w := callKeyed(s, `{"model":"preferred","messages":[{"role":"user","content":"different"}]}`, "key-2")
	if w.Code != 422 {
		t.Fatalf("status = %d, want 422; a different body got the first answer", w.Code)
	}
}

// The case the feature exists for: a transport error leaves generation genuinely
// ambiguous, so the key must stay held rather than let a retry pay again.
func TestUnknownOutcomeIsStickyAndRefusesRetry(t *testing.T) {
	s := idemServer(t, map[string]ProviderConfig{"openai": {URL: "https://openai.test", KeyEnv: "PROVIDER_KEY"}})
	s.HTTP.Transport = &failingTransport{}

	if w := callKeyed(s, chat, "key-3"); w.Code != 502 {
		t.Fatalf("first attempt: %d %s", w.Code, w.Body)
	}
	w := callKeyed(s, chat, "key-3")
	if w.Code != 409 {
		t.Fatalf("retry after an unknown outcome returned %d, want 409; it could charge twice", w.Code)
	}
	if !strings.Contains(w.Body.String(), "unknown") {
		t.Errorf("the refusal does not explain why: %s", w.Body)
	}
	if s.Metrics.IdempotentUnknown.Load() != 1 {
		t.Errorf("IdempotentUnknown = %d, want 1", s.Metrics.IdempotentUnknown.Load())
	}
}

// Holding a key after an unambiguous failure would be wrong in the other
// direction: the caller can fix the request, and should be able to retry.
func TestUnambiguousFailureReleasesTheKey(t *testing.T) {
	var calls atomic.Int64
	fail := true
	h := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if fail {
			w.WriteHeader(400)
			io.WriteString(w, `{"error":{"message":"messages: at least one message is required"}}`)
			return
		}
		io.WriteString(w, success)
	}))
	defer h.Close()
	s := idemServer(t, map[string]ProviderConfig{"openai": {URL: h.URL, KeyEnv: "PROVIDER_KEY"}})

	if w := callKeyed(s, chat, "key-4"); w.Code != 400 {
		t.Fatalf("first attempt: %d", w.Code)
	}
	fail = false
	if w := callKeyed(s, chat, "key-4"); w.Code != 200 {
		t.Fatalf("retry after a provider 400 returned %d; the key was burned by a fixable error", w.Code)
	}
	if calls.Load() != 2 {
		t.Errorf("provider calls = %d, want 2", calls.Load())
	}
}

// Two requests arriving together must not both reach the provider.
func TestConcurrentDuplicatesReachTheProviderOnce(t *testing.T) {
	var calls atomic.Int64
	h := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(40 * time.Millisecond)
		io.WriteString(w, success)
	}))
	defer h.Close()
	s := idemServer(t, map[string]ProviderConfig{"openai": {URL: h.URL, KeyEnv: "PROVIDER_KEY"}})

	// Two, not more: testServer sets concurrency to 2, and extra goroutines would
	// be shed with 429 by the admission limiter before idempotency ever saw them.
	var wg sync.WaitGroup
	codes := make([]int, 2)
	for i := range codes {
		wg.Add(1)
		go func(i int) { defer wg.Done(); codes[i] = callKeyed(s, chat, "key-5").Code }(i)
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("provider called %d times for one key", calls.Load())
	}
	ok, conflict := 0, 0
	for _, c := range codes {
		switch c {
		case 200:
			ok++
		case 409:
			conflict++
		}
	}
	if ok != 1 || ok+conflict != len(codes) {
		t.Fatalf("codes = %v; want exactly one 200 and the rest 409", codes)
	}
}

// Off unless configured: entries hold customer content, so an upgrade must not
// silently start storing it.
func TestIdempotencyRefusedWhenNotConfigured(t *testing.T) {
	h := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, success) }))
	defer h.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: h.URL, KeyEnv: "PROVIDER_KEY"}})
	w := callKeyed(s, chat, "key-6")
	if w.Code != 400 || !strings.Contains(w.Body.String(), "idempotency_ttl_seconds") {
		t.Fatalf("status %d, body %s; want a 400 naming the setting that enables it", w.Code, w.Body)
	}
}

func TestExpiredEntryIsTreatedAsAbsent(t *testing.T) {
	var calls atomic.Int64
	h := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.WriteString(w, success)
	}))
	defer h.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: h.URL, KeyEnv: "PROVIDER_KEY"}})
	store, err := NewIdemStore(t.TempDir(), time.Nanosecond, 1<<20, s.Metrics)
	if err != nil {
		t.Fatal(err)
	}
	s.Idem = store

	callKeyed(s, chat, "key-7")
	time.Sleep(2 * time.Millisecond)
	if w := callKeyed(s, chat, "key-7"); w.Code != 200 {
		t.Fatalf("expired entry did not behave as absent: %d", w.Code)
	}
	if calls.Load() != 2 {
		t.Errorf("provider calls = %d, want 2", calls.Load())
	}
}

// A replayed stream carries the same answer, not the original frame timing.
// docs/API.md says so; this asserts the answer part.
func TestStreamReplayDeliversTheSameAnswer(t *testing.T) {
	var calls atomic.Int64
	h := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"one two\"}}\n\n")
		io.WriteString(w, "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n")
		io.WriteString(w, "data: {\"type\":\"message_stop\"}\n\n")
	}))
	defer h.Close()
	s := idemServer(t, map[string]ProviderConfig{"anthropic": {URL: h.URL, KeyEnv: "PROVIDER_KEY"}})
	body := `{"model":"preferred","stream":true,"messages":[{"role":"user","content":"hi"}]}`

	first := callKeyed(s, body, "stream-key")
	if first.Code != 200 || !strings.Contains(first.Body.String(), "one two") {
		t.Fatalf("first stream: %d %s", first.Code, first.Body)
	}
	second := callKeyed(s, body, "stream-key")
	if calls.Load() != 1 {
		t.Fatalf("provider called %d times; the replayed stream was charged", calls.Load())
	}
	if !strings.Contains(second.Body.String(), "one two") {
		t.Errorf("replayed stream lost the answer: %s", second.Body)
	}
	if !strings.Contains(second.Body.String(), "[DONE]") {
		t.Errorf("replayed stream has no terminator: %s", second.Body)
	}
	if second.Header().Get("X-Switchboard-Replayed") != "true" {
		t.Error("replayed stream is indistinguishable from a fresh one")
	}
}

// The store filling was not a housekeeping problem, it was the feature turning
// itself off. Expiry ran only in NewIdemStore, so a process that stayed up never
// reclaimed a byte: finish() overwrites and never deletes, and only release()
// decrements used. Once used crossed the limit, every new key was refused,
// Server.chat did not match that error, and the request proceeded with no entry
// at all -- so an ambiguous retry was billed twice, while replay, conflict and
// unknown all still read zero.
func TestIdemSweepReclaimsExpiredEntries(t *testing.T) {
	dir := t.TempDir()
	m := &Metrics{}
	s, err := NewIdemStore(dir, 50*time.Millisecond, 1<<20, m)
	if err != nil {
		t.Fatal(err)
	}

	body := []byte(`{"model":"m"}`)
	for _, k := range []string{"a", "b", "c"} {
		if _, err := s.begin(k, body); err != nil {
			t.Fatalf("begin %s: %v", k, err)
		}
		s.finish(k, &idemEntry{
			State: idemDone, BodyHash: hashBody(body), Stored: time.Now().Unix(),
			Status: 200, Response: []byte(`{"ok":true}`),
		})
	}
	if s.used == 0 || s.count != 3 {
		t.Fatalf("after three settled entries: used=%d count=%d, want non-zero and 3",
			s.used, s.count)
	}

	time.Sleep(80 * time.Millisecond)
	s.Sweep()

	if s.count != 0 || s.used != 0 {
		t.Errorf("after Sweep: used=%d count=%d, want 0 and 0; expired entries were "+
			"settled, and settled entries are exactly the ones nothing else reclaims",
			s.used, s.count)
	}
	if names, _ := os.ReadDir(dir); len(names) != 0 {
		t.Errorf("%d expired files still on disk; docs/SECURITY.md says entries expire "+
			"with the TTL", len(names))
	}
}

// The failure this guards, end to end: fill the store with settled entries until
// it refuses a new key, then confirm a sweep restores service. Without Sweep the
// second begin() stays refused for the life of the process.
func TestIdemFullStoreRecoversAfterSweep(t *testing.T) {
	s, err := NewIdemStore(t.TempDir(), 50*time.Millisecond, 400, &Metrics{})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"model":"m"}`)
	settle := func(k string) error {
		if _, err := s.begin(k, body); err != nil {
			return err
		}
		s.finish(k, &idemEntry{
			State: idemDone, BodyHash: hashBody(body), Stored: time.Now().Unix(),
			Status: 200, Response: []byte(`{"ok":true}`),
		})
		return nil
	}
	if err := settle("first"); err != nil {
		t.Fatalf("first key: %v", err)
	}
	// Enough to cross the 400-byte limit.
	var refused error
	for i := 0; i < 20 && refused == nil; i++ {
		refused = settle(fmt.Sprintf("k%d", i))
	}
	if refused == nil {
		t.Skip("store did not fill; nothing to recover from")
	}

	time.Sleep(80 * time.Millisecond)
	s.Sweep()

	if _, err := s.begin("after-sweep", body); err != nil {
		t.Errorf("still refusing new keys after Sweep: %v. Until this passes, a full "+
			"store means idempotency is off and duplicate requests are billed twice", err)
	}
}

// A sweep must not hold the store mutex across the directory scan.
//
// It did, once a minute, while begin, finish, release and write contend on the
// same mutex from the request path -- so every retry-bearing request queued
// behind a full ReadDir. capture.go had already rejected the pattern and named
// this file as where it came from.
//
// Asserted by arithmetic rather than by timing: a timing test on a mutex is
// flaky on a loaded machine and proves nothing on a fast one. The sweep is given
// more expired entries than one pass may take, and the bound is what a locked,
// unbounded loop cannot satisfy.
func TestSweepIsBoundedPerPass(t *testing.T) {
	dir := t.TempDir()
	s, err := NewIdemStore(dir, time.Millisecond, 1<<30, &Metrics{})
	if err != nil {
		t.Fatal(err)
	}
	const n = idemSweepPerPass + 250
	old := time.Now().Add(-time.Hour)
	for i := 0; i < n; i++ {
		p := filepath.Join(dir, fmt.Sprintf("%064x", i))
		if err := os.WriteFile(p, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}

	s.Sweep()
	left, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if want := n - idemSweepPerPass; len(left) != want {
		t.Errorf("after one sweep %d entries remain, want %d; the pass is not bounded, "+
			"so a tick costs O(directory) and blocks every request that takes the mutex",
			len(left), want)
	}
	// And it converges: the remainder goes on the next pass.
	s.Sweep()
	if left, _ := os.ReadDir(dir); len(left) != 0 {
		t.Errorf("%d entries survived a second sweep", len(left))
	}
}

// The request path must keep working while a sweep runs. Under -race this also
// covers the accounting, which is now updated outside the scan.
func TestSweepDoesNotBlockTheRequestPath(t *testing.T) {
	dir := t.TempDir()
	s, err := NewIdemStore(dir, time.Millisecond, 1<<30, &Metrics{})
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	for i := 0; i < 500; i++ {
		p := filepath.Join(dir, fmt.Sprintf("%064x", i))
		os.WriteFile(p, []byte("{}"), 0o600)
		os.Chtimes(p, old, old)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Sweep()
	}()
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("live-%d", i)
		if _, err := s.begin(key, []byte(`{"a":1}`)); err != nil {
			t.Errorf("begin during sweep: %v", err)
			break
		}
		s.release(key)
	}
	<-done
}
