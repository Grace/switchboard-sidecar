// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// emitted runs one request and returns the Event the gateway reported for it.
//
// Through the real route loop and the real Emit, rather than by building an
// Event by hand. A test that constructs the struct it is asserting on proves
// only that the struct can hold the values -- which is how two mutation checks
// in this repository previously passed while the wiring they described was
// broken.
func emitted(t *testing.T, s *Server, body string) Event {
	t.Helper()
	tel, err := NewTelemetry(Config{DataDir: t.TempDir(), ControlURL: "http://127.0.0.1:1", QueueSize: 16}, s.Metrics)
	if err != nil {
		t.Fatal(err)
	}
	useTestTransport(tel.http)
	s.Telemetry = tel

	if w := call(s, body); w.Code != 200 {
		t.Fatalf("request failed: %d %s", w.Code, w.Body)
	}
	select {
	case e := <-tel.queue:
		return e
	default:
		t.Fatal("no event was emitted")
	}
	return Event{}
}

// TestFailoverReportsWhatEveryAttemptCost is the regression test for a bug that
// under-reported real spend.
//
// A reasoning model can burn its whole budget on hidden reasoning and return no
// text, billed in full -- 9 tokens in, 1024 spent reasoning, nothing out. The
// gateway fails over, a second provider answers, and the caller is served.
//
// The cost of the first attempt used to vanish. event.Usage was assigned on each
// attempt, so the second overwrote the first, and the span reported only what
// the provider that succeeded charged. The comment above that line said the
// opposite -- that an answer which was billed and then rejected still had to be
// recorded -- which is what made it hard to see.
//
// /v1/savings aggregates exactly this number, so the error was not academic: a
// gateway whose whole purpose is failing over reported the cost of failing over
// as zero.
func TestFailoverReportsWhatEveryAttemptCost(t *testing.T) {
	first := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, emptyByBudget) // 200, billed, no text
	}))
	defer first.Close()
	second := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"content":[{"type":"text","text":"rescued"}],"stop_reason":"end_turn",`+
			`"usage":{"input_tokens":41,"output_tokens":12}}`)
	}))
	defer second.Close()

	s := testServer(t, map[string]ProviderConfig{
		"openai":    {URL: first.URL, KeyEnv: "PROVIDER_KEY"},
		"anthropic": {URL: second.URL, KeyEnv: "PROVIDER_KEY"},
	})
	e := emitted(t, s, chat)

	if e.Attempts != 2 {
		t.Fatalf("Attempts = %d, want 2; the fixture no longer fails over", e.Attempts)
	}
	if len(e.Tries) != e.Attempts {
		t.Fatalf("len(Tries) = %d, Attempts = %d; every upstream call must have a record",
			len(e.Tries), e.Attempts)
	}

	// The first attempt was billed for 9 in and 1024 reasoning, and produced
	// nothing. The second charged 41 and 12.
	if got := e.Tries[0].Usage; got.Input != 9 || got.Output != 1024 || got.Reasoning != 1024 {
		t.Errorf("attempt 1 usage = %+v, want input 9, output 1024, reasoning 1024", got)
	}
	if got := e.Tries[1].Usage; got.Input != 41 || got.Output != 12 {
		t.Errorf("attempt 2 usage = %+v, want input 41 and output 12", got)
	}

	// The number the bill is reconstructed from.
	if e.Usage.Input != 9+41 {
		t.Errorf("Usage.Input = %d, want %d (9 billed by the attempt that produced "+
			"nothing, plus 41 by the one that answered). Reporting only the "+
			"second is the bug this test exists for", e.Usage.Input, 9+41)
	}
	if e.Usage.Reasoning != 1024 {
		t.Errorf("Usage.Reasoning = %d, want 1024; the tokens the first provider "+
			"spent reasoning were still billed", e.Usage.Reasoning)
	}
	// 1024 + 12, not 12. The provider that produced no visible text still
	// emitted 1024 completion tokens -- OpenAI counts reasoning inside
	// completion_tokens -- and they were billed. Those are the exact tokens the
	// overwrite used to discard, so this is the assertion that would have caught
	// it.
	if e.Usage.Output != 1024+12 {
		t.Errorf("Usage.Output = %d, want %d (1024 burnt by the attempt that "+
			"produced nothing, plus 12 by the one that answered)",
			e.Usage.Output, 1024+12)
	}

	// Each attempt is attributed to the provider that charged it, or a savings
	// report can total correctly and still bill the wrong account.
	if e.Tries[0].Provider != "openai" || e.Tries[1].Provider != "anthropic" {
		t.Errorf("attempts attributed to %q then %q, want openai then anthropic",
			e.Tries[0].Provider, e.Tries[1].Provider)
	}
	for i, tr := range e.Tries {
		if tr.SpanID == "" || len(tr.SpanID) != 16 {
			t.Errorf("attempt %d has span id %q; a child span needs one", i+1, tr.SpanID)
		}
		if tr.Seq != i+1 {
			t.Errorf("attempt %d has Seq %d", i+1, tr.Seq)
		}
	}
	if e.Tries[0].SpanID == e.Tries[1].SpanID {
		t.Error("both attempts share a span id")
	}
}

// TestASingleAttemptStillReportsItsCost guards the ordinary path, so the change
// above cannot be right only for failover.
func TestASingleAttemptStillReportsItsCost(t *testing.T) {
	p := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"choices":[{"index":0,"message":{"content":"hi"},"finish_reason":"stop"}],`+
			`"usage":{"prompt_tokens":7,"completion_tokens":3}}`)
	}))
	defer p.Close()

	s := testServer(t, map[string]ProviderConfig{"openai": {URL: p.URL, KeyEnv: "PROVIDER_KEY"}})
	e := emitted(t, s, chat)

	if len(e.Tries) != 1 {
		t.Fatalf("len(Tries) = %d, want 1", len(e.Tries))
	}
	if e.Usage.Input != 7 || e.Usage.Output != 3 {
		t.Errorf("Usage = %+v, want input 7 output 3", e.Usage)
	}
	if e.Tries[0].Usage != e.Usage {
		t.Errorf("with one attempt the total and the attempt must agree: %+v vs %+v",
			e.Tries[0].Usage, e.Usage)
	}
}

// TestARefusedRequestRecordsWhatItAskedFor covers the gap that made a refusal
// countable but not actionable.
//
// Event.Model is the model sent upstream and is set once a route is chosen, so a
// request the policy refuses carried no model at all. The savings view could
// report that N requests never reached a provider and nothing about what any of
// them wanted -- and "a rising refusal rate" is only useful next to "they are
// all asking for a model the policy dropped".
func TestARefusedRequestRecordsWhatItAskedFor(t *testing.T) {
	p := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("the provider was called for a model the policy does not offer")
	}))
	defer p.Close()

	s := testServer(t, map[string]ProviderConfig{"openai": {URL: p.URL, KeyEnv: "PROVIDER_KEY"}})
	tel, err := NewTelemetry(Config{DataDir: t.TempDir(), ControlURL: "http://127.0.0.1:1", QueueSize: 16}, s.Metrics)
	if err != nil {
		t.Fatal(err)
	}
	useTestTransport(tel.http)
	s.Telemetry = tel

	const asked = "a-model-no-policy-offers"
	w := call(s, `{"model":"`+asked+`","messages":[{"role":"user","content":"hi"}]}`)
	if w.Code < 400 {
		t.Fatalf("expected a refusal, got %d %s", w.Code, w.Body)
	}
	t.Logf("refusal: %d %s", w.Code, strings.TrimSpace(w.Body.String()))

	select {
	case e := <-tel.queue:
		if e.Attempts != 0 {
			t.Errorf("Attempts = %d; a refused request reached a provider", e.Attempts)
		}
		if e.Model != "" {
			t.Errorf("Model = %q; nothing was sent upstream, so there is no upstream model", e.Model)
		}
		if e.Requested != asked {
			t.Errorf("Requested = %q, want %q -- without it the dashboard can count "+
				"refusals and not say what they wanted", e.Requested, asked)
		}
		if got := e.wire().Ext["requested_model"]; got != asked {
			t.Errorf("ext[requested_model] = %v, want %q; it has to reach the control "+
				"plane or only the span has it", got, asked)
		}
	default:
		t.Fatal("no event was emitted for a refused request")
	}
}

// TestTheRouteHeaderNamesEveryAttempt covers the one thing a caller cannot learn
// from the other headers.
//
// X-Switchboard-Provider says which provider answered and X-Switchboard-Attempts
// says how many were made. Neither says what went wrong first, so a caller whose
// request came back slowly from an unexpected provider had to ask somebody with
// access to the telemetry spool.
func TestTheRouteHeaderNamesEveryAttempt(t *testing.T) {
	first := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, emptyByBudget)
	}))
	defer first.Close()
	second := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"content":[{"type":"text","text":"rescued"}],"stop_reason":"end_turn"}`)
	}))
	defer second.Close()

	s := testServer(t, map[string]ProviderConfig{
		"openai":    {URL: first.URL, KeyEnv: "PROVIDER_KEY"},
		"anthropic": {URL: second.URL, KeyEnv: "PROVIDER_KEY"},
	})
	w := call(s, chat)
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}

	got := w.Header().Get("X-Switchboard-Route")
	if got == "" {
		t.Fatal("no X-Switchboard-Route on a request that failed over")
	}
	// Order is the information: the last entry answered, the ones before it are
	// why the request took as long as it did.
	if !strings.HasPrefix(got, "openai/") {
		t.Errorf("route = %q; the first attempt must come first", got)
	}
	if !strings.Contains(got, "anthropic/") {
		t.Errorf("route = %q; the provider that answered is missing", got)
	}
	if strings.Index(got, "openai/") > strings.Index(got, "anthropic/") {
		t.Errorf("route = %q; attempts are out of order", got)
	}
	if n := strings.Count(got, ","); n != 1 {
		t.Errorf("route = %q; want exactly two attempts", got)
	}
	// It has to agree with the count beside it, or the two headers describe
	// different requests.
	if a := w.Header().Get("X-Switchboard-Attempts"); a != "2" {
		t.Errorf("X-Switchboard-Attempts = %q, want 2 alongside route %q", a, got)
	}
}

// TestTheRouteHeaderOnAnOrdinaryRequest keeps the common case honest: one
// attempt, one entry, and no comma implying a failover that did not happen.
func TestTheRouteHeaderOnAnOrdinaryRequest(t *testing.T) {
	p := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"choices":[{"index":0,"message":{"content":"hi"},"finish_reason":"stop"}],`+
			`"usage":{"prompt_tokens":7,"completion_tokens":3}}`)
	}))
	defer p.Close()

	s := testServer(t, map[string]ProviderConfig{"openai": {URL: p.URL, KeyEnv: "PROVIDER_KEY"}})
	w := call(s, chat)
	got := w.Header().Get("X-Switchboard-Route")
	if strings.Contains(got, ",") {
		t.Errorf("route = %q on a single-attempt request", got)
	}
	if !strings.Contains(got, ":200") {
		t.Errorf("route = %q; want the upstream status", got)
	}
}
