package gateway

// Conformance checked from outside this repository.
//
// Every other test of the GenAI attributes asserts the emitter against tables
// this repository also wrote, which is a closed loop: the provider enum is
// checked against the same four strings the code emits, so a wrong pair agrees
// with itself. Item 23 of docs/GAPS.md is exactly that failure -- two of four
// providers were named off-enum for as long as spans have existed, and nothing
// here noticed, because nothing here knew.
//
// genai-interlingua is an independent implementation of what the conventions
// say, built from spans captured off six other instrumentation libraries. It has
// never heard of Switchboard. Pointing it at this gateway's output asks a
// question the rest of the suite cannot: does a tool that only knows the
// conventions find anything wrong here?
//
// A conformant emitter should give a normalizer nothing to do. Concretely:
//
//   - interlingua.dialect comes back "raw" -- these spans are native gen_ai.*,
//     not one of the dialects it translates. If it ever reports "openllmetry",
//     something has gone wrong in one of the two projects.
//   - interlingua.lossy is empty. That attribute names the keys a span is not a
//     faithful carrier of, and it is where the off-enum provider shows up: a
//     span carrying gen_ai.provider.name="bedrock" is reported lossy on that
//     key, and the same span with "aws.bedrock" is not.
//
// Skipped unless the binary is present, so the suite still runs without it:
//
//	go install github.com/Grace/genai-interlingua/cmd/interlingua@latest
//
// A skipped test proves nothing, which is what SWITCHBOARD_REQUIRE_INTERLINGUA
// is for -- set it and a missing binary is a failure rather than a shrug.

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// interlinguaTarget is pinned rather than defaulted, for the reason that project
// documents at length in docs/moving-target.md and that now applies to this one
// too: gen_ai.* was deprecated out of semantic-conventions at v1.42.0 and moved
// to semantic-conventions-genai, which has no tagged release. There is no
// version to normalize to. v1.41.0 is the last tag whose gen_ai.* definitions
// were live, so it is the only target a reader can reconstruct later.
const interlinguaTarget = "v1.41.0"

func interlinguaBin(t *testing.T) string {
	t.Helper()
	if p, err := exec.LookPath("interlingua"); err == nil {
		return p
	}
	if home, err := os.UserHomeDir(); err == nil {
		p := home + "/go/bin/interlingua"
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	if os.Getenv("SWITCHBOARD_REQUIRE_INTERLINGUA") != "" {
		t.Fatal("interlingua not found and SWITCHBOARD_REQUIRE_INTERLINGUA is set;\n" +
			"  go install github.com/Grace/genai-interlingua/cmd/interlingua@latest")
	}
	t.Skip("interlingua not installed; go install github.com/Grace/genai-interlingua/cmd/interlingua@latest")
	return ""
}

// normalizeThroughInterlingua pipes an OTLP/JSON export through the CLI and
// returns each span's interlingua.* verdict, keyed by span name.
func normalizeThroughInterlingua(t *testing.T, bin string, export map[string]any) []map[string]any {
	t.Helper()
	cmd := exec.Command(bin, "-target", interlinguaTarget)
	cmd.Stdin = bytes.NewReader(jsonBytes(export))
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("interlingua failed: %v\n%s", err, errb.String())
	}
	var decoded struct {
		ResourceSpans []struct {
			ScopeSpans []struct {
				Spans []map[string]any `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("interlingua output is not OTLP JSON: %v\n%s", err, out.String())
	}
	var spans []map[string]any
	for _, rs := range decoded.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			spans = append(spans, ss.Spans...)
		}
	}
	return spans
}

func verdict(span map[string]any) (dialect string, lossy []string) {
	for _, a := range span["attributes"].([]any) {
		m := a.(map[string]any)
		v := m["value"].(map[string]any)
		switch m["key"] {
		case "interlingua.dialect":
			dialect, _ = v["stringValue"].(string)
		case "interlingua.lossy":
			arr, ok := v["arrayValue"].(map[string]any)
			if !ok {
				// Emitted as a bare list by some versions; accept either.
				if vals, ok := v["values"].([]any); ok {
					arr = map[string]any{"values": vals}
				} else {
					continue
				}
			}
			for _, e := range arr["values"].([]any) {
				if s, ok := e.(map[string]any)["stringValue"].(string); ok {
					lossy = append(lossy, s)
				}
			}
		}
	}
	return dialect, lossy
}

func TestSpansAreConformantAccordingToInterlingua(t *testing.T) {
	bin := interlinguaBin(t)
	tel := &Telemetry{c: Config{}, m: &Metrics{}}

	// One per provider, plus the shapes that differ: a refusal carrying
	// error.type, a request that never reached a provider and so has no model or
	// usage, and a cached Anthropic call, which is the only one that fills the
	// cache attributes.
	events := []Event{
		// With a served model that differs from the one sent, so interlingua
		// judges gen_ai.response.model as well as gen_ai.request.model.
		{TraceID: "t", SpanID: "s", Provider: "openai", Model: "gpt-5-nano", Status: 200,
			Tries: []attemptRecord{{Provider: "openai", Model: "gpt-5-nano", ResponseModel: "gpt-5-nano-2025-08-07"}},
			Start: 1, End: 2, Attempts: 1, Usage: tokenUsage{Input: 30, Output: 12, Reasoning: 8}},
		{TraceID: "t", SpanID: "s", Provider: "anthropic", Model: "claude-haiku-4-5", Status: 200,
			Start: 1, End: 2, Attempts: 1, Usage: tokenUsage{Input: 100050, Output: 7, CacheRead: 100000}},
		{TraceID: "t", SpanID: "s", Provider: "gemini", Model: "gemini-3.6-flash", Status: 200,
			Start: 1, End: 2, Attempts: 2},
		{TraceID: "t", SpanID: "s", Provider: "bedrock", Model: "claude-sonnet-4", Status: 503,
			Start: 1, End: 2, Attempts: 3, Fault: faultDegraded.String()},
		{TraceID: "t", SpanID: "s", Provider: "", Status: 403, Start: 1, End: 2},
	}
	spans := make([]any, 0, len(events))
	for _, e := range events {
		spans = append(spans, tel.spanOf(e))
	}
	export := map[string]any{"resourceSpans": []any{map[string]any{
		"resource":   map[string]any{"attributes": []any{}},
		"scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "switchboard"}, "spans": spans}},
	}}}

	got := normalizeThroughInterlingua(t, bin, export)
	if len(got) != len(events) {
		t.Fatalf("got %d spans back, sent %d", len(got), len(events))
	}
	for i, span := range got {
		dialect, lossy := verdict(span)

		// A request that never reached a provider is not a GenAI operation, so it
		// carries no gen_ai.* at all and interlingua rightly declines to classify
		// it. The invariant there is the absence, not the verdict.
		if events[i].Provider == "" {
			for _, a := range span["attributes"].([]any) {
				if k := a.(map[string]any)["key"].(string); strings.HasPrefix(k, "gen_ai.") {
					t.Errorf("span %d reached no provider yet claims %s", i, k)
				}
			}
			if dialect != "" {
				t.Errorf("span %d reached no provider yet was classified as %q", i, dialect)
			}
			continue
		}

		// "raw" is its name for a span already in the conventions' vocabulary.
		// Anything else means it recognised this gateway's output as some other
		// library's dialect, which would be a finding in itself.
		if dialect != "raw" {
			t.Errorf("span %d (%s): detected as dialect %q, want \"raw\"",
				i, events[i].Provider, dialect)
		}
		// The assertion that matters. Each entry is a key this span does not
		// faithfully carry -- and an off-enum gen_ai.provider.name lands here.
		if len(lossy) > 0 {
			t.Errorf("span %d (provider=%q model=%q status=%d): interlingua reports it is not a "+
				"faithful carrier of %v\n  That is a conformance defect in this gateway, found by a "+
				"tool that has never heard of it.",
				i, events[i].Provider, events[i].Model, events[i].Status, lossy)
		}
	}
}
