package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStreams(t *testing.T) {
	cases := map[string]string{
		"openai":    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\r\n\r\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
		"anthropic": "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n",
		"gemini":    "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\"}]}}]}\n\ndata: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n",
	}
	for provider, stream := range cases {
		t.Run(provider, func(t *testing.T) {
			s := testServer(t, nil)
			w := httptest.NewRecorder()
			res := &http.Response{Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(stream))}
			if e := s.stream(w, res, Route{provider, "test"}, "id", 1, nil); e != nil {
				t.Fatal(e)
			}
			if !strings.Contains(w.Body.String(), "hello") || strings.Count(w.Body.String(), "[DONE]") != 1 {
				t.Fatal(w.Body.String())
			}
		})
	}
}
func TestStreamTruncation(t *testing.T) {
	s := testServer(t, nil)
	w := httptest.NewRecorder()
	res := &http.Response{Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"))}
	if s.stream(w, res, Route{"openai", "test"}, "id", 1, nil) == nil || strings.Contains(w.Body.String(), "[DONE]") || !strings.Contains(w.Body.String(), "stream_error") {
		t.Fatal(w.Body.String())
	}
}
func TestSSEMultilineAndBound(t *testing.T) {
	var value string
	if e := readSSE(strings.NewReader(": ping\n\ndata: {\ndata: \"a\":1}\n\n"), func(b []byte) error { value = string(b); return nil }); e != nil || value != "{\n\"a\":1}" {
		t.Fatal(value, e)
	}
	if readSSE(strings.NewReader("data: "+strings.Repeat("x", 1<<20)+"\n\n"), func([]byte) error { return nil }) == nil {
		t.Fatal("oversized event accepted")
	}
}
func TestUnexpectedTools(t *testing.T) {
	for provider, b := range map[string]string{"openai": `{"choices":[{"message":{"tool_calls":[{}]},"finish_reason":"tool_calls"}]}`, "anthropic": `{"content":[{"type":"tool_use"}],"stop_reason":"tool_use"}`, "gemini": `{"candidates":[{"content":{"parts":[{"functionCall":{}}]},"finishReason":"STOP"}]}`} {
		if _, _, e := normalize(provider, []byte(b), false); e == nil {
			t.Fatal("tool accepted", provider)
		}
	}
}

// Gemini bills internal reasoning as output tokens, reports it in a field this
// gateway did not model, and may omit candidatesTokenCount entirely. Reading
// only candidatesTokenCount metered a 111-token request as zero output.
//
// Both bodies below are verbatim usageMetadata from real gemini-3.6-flash calls.
func TestNormalizeGeminiCountsThinkingTokens(t *testing.T) {
	for _, tc := range []struct {
		name          string
		usage         string
		input, output int
		mismatch      bool
	}{{
		// candidatesTokenCount absent altogether: the whole budget went to
		// thinking and no visible answer was produced.
		name:   "candidates absent",
		usage:  `{"promptTokenCount":8,"thoughtsTokenCount":103,"totalTokenCount":111}`,
		input:  8,
		output: 103,
	}, {
		// Both present. Reading candidatesTokenCount alone would report 6
		// against 212 tokens actually billed as output.
		name:   "candidates and thoughts",
		usage:  `{"promptTokenCount":6,"candidatesTokenCount":6,"thoughtsTokenCount":206,"totalTokenCount":218}`,
		input:  6,
		output: 212,
	}, {
		// A provider total that does not agree with the parts means this
		// gateway is billing on an accounting model the provider has changed.
		name:     "totals disagree",
		usage:    `{"promptTokenCount":10,"candidatesTokenCount":5,"thoughtsTokenCount":5,"totalTokenCount":999}`,
		input:    10,
		output:   10,
		mismatch: true,
	}, {
		// A stream frame with no usage at all is not a disagreement.
		name:  "usage absent",
		usage: `{}`,
	}, {
		// Nor is a partial frame carrying a total but no prompt count. Reporting
		// either as a mismatch would make the metric noise instead of signal.
		name:   "partial frame",
		usage:  `{"totalTokenCount":40}`,
		output: 0,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"candidates":[{"index":0,"content":{"parts":[{"text":"ok"}]},` +
				`"finishReason":"STOP"}],"usageMetadata":` + tc.usage + `}`
			n, _, err := normalize("gemini", []byte(body), false)
			if err != nil {
				t.Fatalf("rejected a real Gemini response: %v", err)
			}
			if n.Input != tc.input || n.Output != tc.output {
				t.Errorf("usage = in %d/out %d, want in %d/out %d",
					n.Input, n.Output, tc.input, tc.output)
			}
			if n.UsageMismatch != tc.mismatch {
				t.Errorf("UsageMismatch = %v, want %v", n.UsageMismatch, tc.mismatch)
			}
		})
	}
}

// A thinking model can exhaust its budget before emitting any visible text,
// returning a content object with no parts key at all. That is a truncated
// response, not a malformed one, and must normalize rather than error.
func TestNormalizeGeminiEmptyContentIsTruncation(t *testing.T) {
	body := `{"candidates":[{"content":{},"finishReason":"MAX_TOKENS","index":0}],` +
		`"usageMetadata":{"promptTokenCount":8,"thoughtsTokenCount":13,"totalTokenCount":21}}`
	n, done, err := normalize("gemini", []byte(body), false)
	if err != nil {
		t.Fatalf("rejected a real truncated response: %v", err)
	}
	if !done {
		t.Error("a response carrying finishReason should be complete")
	}
	if n.Finish != "length" {
		t.Errorf("Finish = %q, want length", n.Finish)
	}
	if n.Text != "" {
		t.Errorf("Text = %q, want empty", n.Text)
	}
	if n.Output != 13 {
		t.Errorf("Output = %d, want 13 thinking tokens", n.Output)
	}
}

// OpenAI reasoning models reject max_tokens outright. Legacy chat models accept
// either name, so max_completion_tokens is the one that works for both.
func TestOpenAIRequestUsesMaxCompletionTokens(t *testing.T) {
	req, err := upstream(context.Background(),
		Chat{MaxTokens: 64, Messages: []Message{{Role: "user", Content: "hi"}}},
		Route{Provider: "openai", Model: "gpt-5-nano"},
		ProviderConfig{URL: "https://api.openai.com", KeyEnv: "PATH"}, nil)
	if err != nil {
		t.Fatalf("could not build request: %v", err)
	}
	raw, _ := io.ReadAll(req.Body)
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if _, present := got["max_tokens"]; present {
		t.Error("body still sends max_tokens, which reasoning models reject")
	}
	if got["max_completion_tokens"] != float64(64) {
		t.Errorf("max_completion_tokens = %v, want 64", got["max_completion_tokens"])
	}
	if got["model"] != "gpt-5-nano" {
		t.Errorf("model = %v", got["model"])
	}
}

// Anthropic's three input counts are disjoint parts of one total, not a total
// and two subsets of it. Their documentation is explicit -- input_tokens is
// "the tokens that come after the last cache breakpoint in your request, not
// all the input tokens you sent" -- and gives
// total_input = cache_read + cache_creation + input.
//
// Reading input_tokens alone is an unbounded under-count rather than a rounding
// error. Anthropic's own example is 100,000 tokens read from cache plus a
// 50-token message, which reports input_tokens = 50; a gateway that stops there
// tells the caller their request cost 50 input tokens when it cost 100,050.
func TestAnthropicCountsCachedInputTokens(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		usage                 string
		want                  int
		wantRead, wantWritten int
	}{
		{
			// The case every request has today. cache_control is opt-in, so both
			// fields are absent, decode to zero, and the total is unchanged. This
			// is the behaviour that must not move.
			name:  "no caching, unchanged",
			usage: `{"input_tokens":11,"output_tokens":7}`,
			want:  11,
		},
		{
			name:        "cache being written",
			usage:       `{"input_tokens":50,"cache_creation_input_tokens":1024,"output_tokens":7}`,
			want:        1074,
			wantWritten: 1024,
		},
		{
			name:     "cache being read",
			usage:    `{"input_tokens":50,"cache_read_input_tokens":100000,"output_tokens":7}`,
			want:     100050, // Anthropic's own worked example.
			wantRead: 100000,
		},
		{
			name: "reading one cache while writing another",
			usage: `{"input_tokens":50,"cache_creation_input_tokens":1024,` +
				`"cache_read_input_tokens":100000,"output_tokens":7}`,
			want:        101074,
			wantRead:    100000,
			wantWritten: 1024,
		},
		{
			// The whole prompt was cached and nothing followed the breakpoint.
			// input_tokens is legitimately zero here, and reporting zero input for
			// a request that processed 4096 tokens is the starkest form of the bug.
			name:     "everything cached, nothing after the breakpoint",
			usage:    `{"input_tokens":0,"cache_read_input_tokens":4096,"output_tokens":7}`,
			want:     4096,
			wantRead: 4096,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":` +
				tc.usage + `}`
			n, _, err := normalize("anthropic", []byte(body), false)
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if n.Input != tc.want {
				t.Errorf("Input = %d, want %d", n.Input, tc.want)
			}
			if n.Output != 7 {
				t.Errorf("Output = %d, want 7; counting cached input must not disturb output",
					n.Output)
			}
			// The sum is the billing figure and the parts are what say whether
			// caching is working. Summing without keeping the parts made a warm
			// prompt and a cold one indistinguishable at every layer above this.
			if n.CacheRead != tc.wantRead {
				t.Errorf("CacheRead = %d, want %d", n.CacheRead, tc.wantRead)
			}
			if n.CacheWrite != tc.wantWritten {
				t.Errorf("CacheWrite = %d, want %d", n.CacheWrite, tc.wantWritten)
			}
			// The parts are inside the total, not additions to it.
			if n.CacheRead+n.CacheWrite > n.Input {
				t.Errorf("parts (%d+%d) exceed the total %d",
					n.CacheRead, n.CacheWrite, n.Input)
			}
		})
	}
}

// The cache fields are Anthropic's. The usage struct is shared across providers,
// so a key that only one provider sends must not change what the others report:
// OpenAI counts prompt_tokens and Gemini promptTokenCount, and neither has any
// notion of a cache breakpoint.
func TestCachedInputCountsAreAnthropicOnly(t *testing.T) {
	for _, tc := range []struct{ provider, body string }{
		{"openai", `{"choices":[{"message":{"content":"hi"},"finish_reason":"stop"}],` +
			`"usage":{"prompt_tokens":11,"completion_tokens":7,` +
			`"cache_creation_input_tokens":9999,"cache_read_input_tokens":9999}}`},
		{"gemini", `{"candidates":[{"content":{"parts":[{"text":"hi"}]},"finishReason":"STOP"}],` +
			`"usageMetadata":{"promptTokenCount":11,"candidatesTokenCount":7,"totalTokenCount":18,` +
			`"cache_creation_input_tokens":9999,"cache_read_input_tokens":9999}}`},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			n, _, err := normalize(tc.provider, []byte(tc.body), false)
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if n.Input != 11 {
				t.Errorf("Input = %d, want 11; %s does not report Anthropic cache fields",
					n.Input, tc.provider)
			}
		})
	}
}
