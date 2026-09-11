// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// stringAttrOf returns a string attribute from a span built by spanOf.
func stringAttrOf(span map[string]any, key string) (string, bool) {
	for _, a := range span["attributes"].([]any) {
		m := a.(map[string]any)
		if m["key"] == key {
			s, ok := m["value"].(map[string]any)["stringValue"].(string)
			return s, ok
		}
	}
	return "", false
}

// TestNormalizeReadsTheServedModel covers the three places providers state the
// model that answered, and the one that states none.
//
// The shapes are from each provider's own responses: Anthropic's message_start
// frame is its documentation's example verbatim, and Gemini names the field
// modelVersion in its GenerateContentResponse reference.
func TestNormalizeReadsTheServedModel(t *testing.T) {
	for _, tc := range []struct {
		name, provider, body string
		stream               bool
		want                 string
	}{
		{name: "openai, complete", provider: "openai",
			body: `{"model":"gpt-4o-mini-2024-07-18","choices":[{"index":0,"message":{"content":"hi"},"finish_reason":"stop"}]}`,
			want: "gpt-4o-mini-2024-07-18"},
		{name: "openai, stream chunk", provider: "openai", stream: true,
			body: `{"model":"gpt-4o-mini-2024-07-18","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}`,
			want: "gpt-4o-mini-2024-07-18"},
		{name: "anthropic, complete", provider: "anthropic",
			body: `{"model":"claude-sonnet-4-20250514","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn"}`,
			want: "claude-sonnet-4-20250514"},
		{name: "anthropic, message_start", provider: "anthropic", stream: true,
			body: `{"type": "message_start", "message": {"id": "msg_1nZdL29xx5MUA1yADyHTEsnR8uuvGzszyY", "type": "message", "role": "assistant", "content": [], "model": "claude-opus-5", "stop_reason": null, "stop_sequence": null, "usage": {"input_tokens": 25, "output_tokens": 1}}}`,
			want: "claude-opus-5"},
		{
			// Every later Anthropic frame is silent about the model. Silence is
			// not a statement that it changed; the stream keeps what message_start
			// said, which TestAStreamKeepsTheServedModelPastSilentFrames checks.
			name: "anthropic, a frame that does not say", provider: "anthropic", stream: true,
			body: `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`,
		},
		{name: "gemini, complete", provider: "gemini",
			body: `{"candidates":[{"content":{"parts":[{"text":"hi"}]},"finishReason":"STOP"}],"modelVersion":"gemini-2.0-flash-001"}`,
			want: "gemini-2.0-flash-001"},
		{
			// Returned before the candidates are read. The prompt was still
			// refused by a particular model.
			name: "gemini, blocked prompt", provider: "gemini",
			body: `{"promptFeedback":{"blockReason":"SAFETY"},"modelVersion":"gemini-2.0-flash-001"}`,
			want: "gemini-2.0-flash-001"},
		{
			// Converse has no model field. Empty is the correct answer.
			name: "bedrock, complete", provider: "bedrock",
			body: `{"output":{"message":{"content":[{"text":"hi"}]}},"stopReason":"end_turn","usage":{"inputTokens":1,"outputTokens":1}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, _, err := normalize(tc.provider, []byte(tc.body), tc.stream)
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if n.Model != tc.want {
				t.Errorf("Model = %q, want %q", n.Model, tc.want)
			}
		})
	}
}

// Message is decoded lazily, for message_start only. A body with some other
// top-level "message" -- a string, as a proxy's error envelope might carry --
// decoded without complaint before the served model was read, and must still.
func TestATopLevelMessageStringIsNotADecodeFailure(t *testing.T) {
	body := `{"message":"hello","choices":[{"index":0,"message":{"content":"hi"},"finish_reason":"stop"}]}`
	n, _, err := normalize("openai", []byte(body), false)
	if err != nil {
		t.Fatalf("normalize: %v; a field added to read the served model broke decoding", err)
	}
	if n.Text != "hi" {
		t.Errorf("Text = %q, want hi", n.Text)
	}
}

// TestTheServedModelReachesTheSpanAndTheControlPlane runs a request through the
// real route loop. The span must carry both models, because the difference
// between them is the signal: test-model was sent and a dated snapshot answered.
func TestTheServedModelReachesTheSpanAndTheControlPlane(t *testing.T) {
	const served = "gpt-4o-mini-2024-07-18"
	p := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"model":"`+served+`","choices":[{"index":0,"message":{"content":"hi"},"finish_reason":"stop"}],`+
			`"usage":{"prompt_tokens":5,"completion_tokens":1}}`)
	}))
	defer p.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: p.URL, KeyEnv: "PROVIDER_KEY"}})
	e := emitted(t, s, chat)

	if len(e.Tries) != 1 || e.Tries[0].ResponseModel != served {
		t.Fatalf("Tries = %+v; want one attempt served by %s", e.Tries, served)
	}

	span := (&Telemetry{c: Config{}, m: &Metrics{}}).spanOf(e)
	if got, _ := stringAttrOf(span, "gen_ai.request.model"); got != "test-model" {
		t.Errorf("gen_ai.request.model = %q, want test-model (what was sent)", got)
	}
	if got, ok := stringAttrOf(span, "gen_ai.response.model"); !ok || got != served {
		t.Errorf("gen_ai.response.model = %q (present %v), want %q", got, ok, served)
	}

	ext := e.wire().Ext
	if ext["response_model"] != served {
		t.Errorf("ext.response_model = %v, want %q", ext["response_model"], served)
	}
	tries, _ := ext["tries"].([]any)
	if len(tries) != 1 || tries[0].(map[string]any)["response_model"] != served {
		t.Errorf("ext.tries = %v; the attempt record does not carry response_model", ext["tries"])
	}
}

// TestEachAttemptKeepsItsOwnServedModel is the failover case. The first provider
// answers with nothing and is billed; the second answers. Each attempt must be
// attributed to the model that actually served it, and the span -- which
// describes the attempt that ended the request -- to the second.
func TestEachAttemptKeepsItsOwnServedModel(t *testing.T) {
	first := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"model":"gpt-5-nano-2025-08-07","choices":[{"index":0,"message":{"content":""},"finish_reason":"length"}],`+
			`"usage":{"prompt_tokens":9,"completion_tokens":1024,"completion_tokens_details":{"reasoning_tokens":1024}}}`)
	}))
	defer first.Close()
	second := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"model":"claude-sonnet-4-20250514","content":[{"type":"text","text":"rescued"}],"stop_reason":"end_turn",`+
			`"usage":{"input_tokens":41,"output_tokens":12}}`)
	}))
	defer second.Close()
	s := testServer(t, map[string]ProviderConfig{
		"openai":    {URL: first.URL, KeyEnv: "PROVIDER_KEY"},
		"anthropic": {URL: second.URL, KeyEnv: "PROVIDER_KEY"},
	})
	e := emitted(t, s, chat)

	if len(e.Tries) != 2 {
		t.Fatalf("len(Tries) = %d, want 2; the fixture no longer fails over", len(e.Tries))
	}
	if got := e.Tries[0].ResponseModel; got != "gpt-5-nano-2025-08-07" {
		t.Errorf("attempt 1 served by %q, want gpt-5-nano-2025-08-07; the attempt that "+
			"produced nothing was still answered by a particular model", got)
	}
	if got := e.Tries[1].ResponseModel; got != "claude-sonnet-4-20250514" {
		t.Errorf("attempt 2 served by %q, want claude-sonnet-4-20250514", got)
	}
	span := (&Telemetry{c: Config{}, m: &Metrics{}}).spanOf(e)
	if got, _ := stringAttrOf(span, "gen_ai.response.model"); got != "claude-sonnet-4-20250514" {
		t.Errorf("span gen_ai.response.model = %q, want the model that answered", got)
	}
}

// TestAProviderThatDoesNotSayLeavesItUnset guards the one tempting shortcut.
// Filling gen_ai.response.model from the request model would make every span
// look complete, and would make an alias silently moving to a new snapshot
// indistinguishable from nothing happening.
func TestAProviderThatDoesNotSayLeavesItUnset(t *testing.T) {
	p := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"choices":[{"index":0,"message":{"content":"hi"},"finish_reason":"stop"}]}`)
	}))
	defer p.Close()
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: p.URL, KeyEnv: "PROVIDER_KEY"}})
	e := emitted(t, s, chat)

	if got := e.Tries[0].ResponseModel; got != "" {
		t.Errorf("ResponseModel = %q for a response that named no model", got)
	}
	span := (&Telemetry{c: Config{}, m: &Metrics{}}).spanOf(e)
	if got, ok := stringAttrOf(span, "gen_ai.response.model"); ok {
		t.Errorf("gen_ai.response.model = %q on a span whose provider did not state one", got)
	}
	if v, ok := e.wire().Ext["response_model"]; ok {
		t.Errorf("ext.response_model = %v on an event whose provider did not state one", v)
	}
}

// TestAStreamKeepsTheServedModelPastSilentFrames streams from Anthropic, which
// names the model once, in message_start, and never again. Every later frame
// normalizes to an empty Model, and none of them may clear the one already
// read.
func TestAStreamKeepsTheServedModelPastSilentFrames(t *testing.T) {
	const stream = "event: message_start\n" +
		`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-opus-5","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":25,"output_tokens":1}}}` + "\n\n" +
		"event: content_block_delta\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}` + "\n\n" +
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}` + "\n\n" +
		`data: {"type":"message_stop"}` + "\n\n"
	p := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, stream)
	}))
	defer p.Close()
	// Only anthropic is configured, so the policy's openai route is passed over
	// and the stream comes from here.
	s := testServer(t, map[string]ProviderConfig{"anthropic": {URL: p.URL, KeyEnv: "PROVIDER_KEY"}})
	e := emitted(t, s, `{"model":"preferred","stream":true,"messages":[{"role":"user","content":"hi"}]}`)

	if len(e.Tries) != 1 {
		t.Fatalf("len(Tries) = %d, want 1", len(e.Tries))
	}
	if got := e.Tries[0].ResponseModel; got != "claude-opus-5" {
		t.Errorf("ResponseModel = %q, want claude-opus-5 from message_start", got)
	}
	span := (&Telemetry{c: Config{}, m: &Metrics{}}).spanOf(e)
	if got, _ := stringAttrOf(span, "gen_ai.response.model"); got != "claude-opus-5" {
		t.Errorf("span gen_ai.response.model = %q, want claude-opus-5", got)
	}
}

// TestAnEmptyStreamStillNamesTheModelThatProducedNothing is the streaming half of
// the failover case. A stream that ends on its budget without a word is caught in
// its own branch, which moves on to the next route before the ordinary stream
// bookkeeping runs. That branch has to record the served model itself, or the
// attempt that burned the budget is the one attempt with no model named -- and it
// is the attempt someone investigating spend asks about first.
func TestAnEmptyStreamStillNamesTheModelThatProducedNothing(t *testing.T) {
	const emptyStream = `data: {"model":"gpt-5-nano-2025-08-07","choices":[{"index":0,"delta":{},"finish_reason":"length"}],` +
		`"usage":{"prompt_tokens":9,"completion_tokens":1024,"completion_tokens_details":{"reasoning_tokens":1024}}}` +
		"\n\ndata: [DONE]\n\n"
	first := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, emptyStream)
	}))
	defer first.Close()
	second := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: message_start\n"+
			`data: {"type":"message_start","message":{"type":"message","role":"assistant","content":[],"model":"claude-opus-5"}}`+"\n\n"+
			`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"rescued"}}`+"\n\n"+
			`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}`+"\n\n"+
			`data: {"type":"message_stop"}`+"\n\n")
	}))
	defer second.Close()
	s := testServer(t, map[string]ProviderConfig{
		"openai":    {URL: first.URL, KeyEnv: "PROVIDER_KEY"},
		"anthropic": {URL: second.URL, KeyEnv: "PROVIDER_KEY"},
	})
	e := emitted(t, s, `{"model":"preferred","stream":true,"max_tokens":1024,"messages":[{"role":"user","content":"hi"}]}`)

	if len(e.Tries) != 2 {
		t.Fatalf("len(Tries) = %d, want 2; the fixture no longer fails over", len(e.Tries))
	}
	if got := e.Tries[0].ResponseModel; got != "gpt-5-nano-2025-08-07" {
		t.Errorf("attempt 1 served by %q, want gpt-5-nano-2025-08-07; the stream named its model "+
			"before producing nothing", got)
	}
	if got := e.Tries[1].ResponseModel; got != "claude-opus-5" {
		t.Errorf("attempt 2 served by %q, want claude-opus-5", got)
	}
}

// TestTheRouteHeaderNamesTheServedModelOnlyWhenItDiffers renders the header from
// attempt records directly, so every shape the grammar has to hold is pinned
// without a provider in the loop.
func TestTheRouteHeaderNamesTheServedModelOnlyWhenItDiffers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event Event
		want  string
	}{
		{"served as sent", Event{Tries: []attemptRecord{
			{Order: 1, Provider: "openai", Model: "gpt-4o-mini", ResponseModel: "gpt-4o-mini", Status: 200}}},
			"openai/gpt-4o-mini:200"},
		{"served by a snapshot", Event{Tries: []attemptRecord{
			{Order: 1, Provider: "openai", Model: "gpt-4o-mini", ResponseModel: "gpt-4o-mini-2024-07-18", Status: 200}}},
			"openai/gpt-4o-mini=gpt-4o-mini-2024-07-18:200"},
		{"an answer that named no model", Event{Tries: []attemptRecord{
			{Order: 1, Provider: "bedrock", Model: "anthropic.claude-3-5-haiku-20241022-v1:0", Status: 200}}},
			"bedrock/anthropic.claude-3-5-haiku-20241022-v1:0=?:200"},
		{"a failed attempt has no answer to name a model in", Event{Tries: []attemptRecord{
			{Order: 1, Provider: "openai", Model: "gpt-4o-mini", Status: 503, Fault: "degraded"}}},
			"openai/gpt-4o-mini:503 degraded"},
		{"a skip between two attempts", Event{
			Tries: []attemptRecord{
				{Order: 1, Provider: "openai", Model: "gpt-4o-mini", Status: 503},
				{Order: 3, Provider: "anthropic", Model: "claude-sonnet-4", ResponseModel: "claude-sonnet-4-20250514", Status: 200}},
			Skipped: []skippedRoute{{Order: 2, Provider: "gemini", Model: "gemini-2.0-flash", Reason: "circuit_open"}}},
			"openai/gpt-4o-mini:503, gemini/gemini-2.0-flash:skipped circuit_open, anthropic/claude-sonnet-4=claude-sonnet-4-20250514:200"},
		{"an empty answer still names its model", Event{Tries: []attemptRecord{
			{Order: 1, Provider: "openai", Model: "gpt-5-nano", ResponseModel: "gpt-5-nano-2025-08-07", Status: 200, Fault: "empty_completion"}}},
			"openai/gpt-5-nano=gpt-5-nano-2025-08-07:200 empty_completion"},
		{
			// The served value comes from the provider. Unsanitised, this one would
			// add a hop, a status and a fault that never happened.
			"a hostile served model", Event{Tries: []attemptRecord{
				{Order: 1, Provider: "openai", Model: "gpt-4o-mini", ResponseModel: "x:200, evil/m=y:200 fault", Status: 200}}},
			"openai/gpt-4o-mini=x:200__evil/m_y:200_fault:200"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.event.routeHeader(); got != tc.want {
				t.Errorf("route header = %q\n            want %q", got, tc.want)
			}
		})
	}
}

// TestTheServedModelReachesTheRouteHeaderACallerReceives is about when, not what.
// The header is first rendered as soon as the provider returns 200, before its
// body has named a model, so it is rendered again just before the first byte.
// Result() is read rather than the recorder's live header map: the recorder
// snapshots headers when the first byte is written, which is what a caller gets,
// and a header set after that would pass a check against the live map.
func TestTheServedModelReachesTheRouteHeaderACallerReceives(t *testing.T) {
	t.Run("complete response", func(t *testing.T) {
		p := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"model":"gpt-4o-mini-2024-07-18","choices":[{"index":0,"message":{"content":"hi"},"finish_reason":"stop"}]}`)
		}))
		defer p.Close()
		s := testServer(t, map[string]ProviderConfig{"openai": {URL: p.URL, KeyEnv: "PROVIDER_KEY"}})
		got := call(s, chat).Result().Header.Get("X-Switchboard-Route")
		if want := "openai/test-model=gpt-4o-mini-2024-07-18:200"; got != want {
			t.Errorf("route header = %q, want %q", got, want)
		}
	})
	t.Run("stream", func(t *testing.T) {
		p := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "event: message_start\n"+
				`data: {"type":"message_start","message":{"type":"message","role":"assistant","content":[],"model":"claude-opus-5"}}`+"\n\n"+
				`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hello"}}`+"\n\n"+
				`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}`+"\n\n"+
				`data: {"type":"message_stop"}`+"\n\n")
		}))
		defer p.Close()
		s := testServer(t, map[string]ProviderConfig{"anthropic": {URL: p.URL, KeyEnv: "PROVIDER_KEY"}})
		got := call(s, `{"model":"preferred","stream":true,"messages":[{"role":"user","content":"hi"}]}`).
			Result().Header.Get("X-Switchboard-Route")
		if want := "anthropic/test-model=claude-opus-5:200"; !strings.HasSuffix(got, want) {
			t.Errorf("route header = %q, want it to end %q", got, want)
		}
	})
}
