package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
type Chat struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Stream      bool      `json:"stream"`
	MaxTokens   int       `json:"max_tokens"`
	Temperature *float64  `json:"temperature,omitempty"`
	// CacheSystem asks the provider to cache the system prompt. Decided by the
	// server from what it has seen before, never by the caller: ParseChat sets
	// extra="forbid" behaviour on unknown keys, and this one carries no JSON tag
	// so a request body cannot reach it. A caller who could set it could make
	// every one-shot prompt pay a cache write.
	CacheSystem bool `json:"-"`
}

// supportedFields is the request surface named once, so an error can say which
// of the caller's fields was refused. The message this replaces recited the
// whole list and mentioned only tools and multimodal, while firing identically
// for top_p, n, stop, seed, stream_options, user and response_format -- so a
// caller who sent response_format was told about tools.
var supportedFields = map[string]bool{
	"model": true, "messages": true, "stream": true, "max_tokens": true, "temperature": true,
}

// parseHint turns a strict-decode failure into something the caller can act on.
// It runs only on the error path, so a well-formed request pays nothing for the
// second decode.
func parseHint(b []byte, e error) string {
	const supported = "Supported fields: model, messages, stream, max_tokens, temperature."
	var top map[string]json.RawMessage
	if json.Unmarshal(b, &top) != nil {
		return "request body is not a JSON object: " + e.Error()
	}
	unknown := make([]string, 0, 4)
	for k := range top {
		if !supportedFields[k] {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		slices.Sort(unknown)
		// Bounded: the field names come from the caller, and this string reaches
		// a response body and the log.
		if len(unknown) > 5 {
			unknown = append(unknown[:5], fmt.Sprintf("and %d more", len(unknown)-5))
		}
		return fmt.Sprintf("unsupported request field(s): %s. %s", strings.Join(unknown, ", "), supported)
	}
	// Only known field names, so the decode failed on a type. Content as an array
	// of parts is by far the most common: every multimodal example sends it.
	return "a supported field has an unsupported value; message content must be a plain string, " +
		"not an array of content parts (" + e.Error() + ")"
}

func ParseChat(b []byte) (Chat, error) {
	var c Chat
	if e := strictJSON(b, &c); e != nil {
		return c, errors.New(parseHint(b, e))
	}
	// Split from the message-count check below. Welded together, the first
	// request an OpenAI SDK user ever sends -- model="gpt-4o" -- was answered
	// with a sentence about message counts, which is not what went wrong.
	if c.Model != "preferred" {
		return c, fmt.Errorf("model must be the literal string \"preferred\", not %q; "+
			"the signed routing policy chooses the provider and model, so the caller does not name one", c.Model)
	}
	if len(c.Messages) < 1 || len(c.Messages) > 128 {
		return c, fmt.Errorf("1-128 messages required, got %d", len(c.Messages))
	}
	if c.MaxTokens == 0 {
		c.MaxTokens = 1024
	}
	if c.MaxTokens < 1 || c.MaxTokens > 16384 || (c.Temperature != nil && (*c.Temperature < 0 || *c.Temperature > 1)) {
		return c, errors.New("invalid generation limits")
	}
	for i, m := range c.Messages {
		if m.Content == "" || (m.Role != "user" && m.Role != "assistant" && m.Role != "system") || (m.Role == "system" && i != 0) {
			return c, errors.New("text messages only; system allowed only first")
		}
	}
	if c.Messages[len(c.Messages)-1].Role != "user" {
		return c, errors.New("last message must be user")
	}
	return c, nil
}
func upstream(ctx context.Context, c Chat, r Route, p ProviderConfig, signer *BedrockSigner) (*http.Request, error) {
	var body any
	path := ""
	system := ""
	msgs := []Message{}
	for _, m := range c.Messages {
		if m.Role == "system" {
			system = m.Content
		} else {
			msgs = append(msgs, m)
		}
	}
	switch r.Provider {
	case "openai":
		// Built explicitly rather than by passing the Chat struct through, because
		// the token limit has to be max_completion_tokens: reasoning models reject
		// max_tokens outright ("Unsupported parameter"), and legacy chat models
		// accept either, so the newer name is the one that works for both. The
		// system message stays inline in messages, which is where OpenAI wants it.
		b := map[string]any{
			"model":                 r.Model,
			"messages":              c.Messages,
			"stream":                c.Stream,
			"max_completion_tokens": c.MaxTokens,
		}
		if c.Temperature != nil {
			b["temperature"] = *c.Temperature
		}
		body = b
		path = "/v1/chat/completions"
	case "anthropic":
		b := map[string]any{"model": r.Model, "messages": msgs, "max_tokens": c.MaxTokens, "stream": c.Stream}
		if system != "" {
			// A bare string unless the prefix is worth caching, because the
			// structured form is only needed to hang cache_control off, and the
			// simplest request that works is the one to send.
			//
			// ephemeral is the only cache type Anthropic defines. The breakpoint
			// goes on the system prompt and nowhere else: it is the part that
			// repeats across callers and turns, and every extra breakpoint is
			// another thing to be wrong about.
			if c.CacheSystem {
				b["system"] = []any{map[string]any{
					"type": "text", "text": system,
					"cache_control": map[string]any{"type": "ephemeral"},
				}}
			} else {
				b["system"] = system
			}
		}
		if c.Temperature != nil {
			b["temperature"] = *c.Temperature
		}
		body = b
		path = "/v1/messages"
	case "gemini":
		contents := []any{}
		for _, m := range msgs {
			role := m.Role
			if role == "assistant" {
				role = "model"
			}
			contents = append(contents, map[string]any{"role": role, "parts": []any{map[string]any{"text": m.Content}}})
		}
		gen := map[string]any{"maxOutputTokens": c.MaxTokens, "candidateCount": 1}
		if c.Temperature != nil {
			gen["temperature"] = *c.Temperature
		}
		b := map[string]any{"contents": contents, "generationConfig": gen}
		if system != "" {
			b["systemInstruction"] = map[string]any{"parts": []any{map[string]any{"text": system}}}
		}
		body = b
		path = "/v1beta/models/" + r.Model + ":generateContent"
		if c.Stream {
			path = "/v1beta/models/" + r.Model + ":streamGenerateContent?alt=sse"
		}
	case "bedrock":
		body = bedrockBody(c, system, msgs)
		// Converse and ConverseStream are separate operations rather than a flag
		// on the body, unlike the other three providers.
		op := "/converse"
		if c.Stream {
			op = "/converse-stream"
		}
		path = "/model/" + url.PathEscape(r.Model) + op
	default:
		return nil, errors.New("unknown adapter")
	}
	raw := jsonBytes(body)
	req, e := http.NewRequestWithContext(ctx, "POST", trimURL(p.URL)+path, bytes.NewReader(raw))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	switch r.Provider {
	case "openai":
		req.Header.Set("Authorization", "Bearer "+os.Getenv(p.KeyEnv))
	case "anthropic":
		req.Header.Set("x-api-key", os.Getenv(p.KeyEnv))
		req.Header.Set("anthropic-version", "2023-06-01")
	case "gemini":
		req.Header.Set("x-goog-api-key", os.Getenv(p.KeyEnv))
	case "bedrock":
		// No key: the task's IAM role authenticates, so nothing long-lived is
		// stored or pasted by a customer.
		if signer == nil {
			return nil, errors.New("bedrock configured but no signer available")
		}
		if e := signer.sign(ctx, req, raw); e != nil {
			return nil, e
		}
	}
	return req, nil
}

type normalized struct {
	Text          string
	Finish        string
	Input, Output int
	// Reasoning is the portion of Output the provider spent on hidden internal
	// reasoning rather than visible text. It is the leading indicator for an
	// empty completion: as it approaches the caller's budget, the answer runs
	// out of room before it is written.
	Reasoning int
	// CacheRead and CacheWrite are the parts of Input that were served from, or
	// written to, a provider-managed prompt cache. They are already inside Input
	// -- the sum is what the request actually cost and is the billing figure --
	// and they are kept apart from it because they cost different amounts and
	// because the conventions name them separately
	// (gen_ai.usage.cache_read.input_tokens / cache_write.input_tokens).
	//
	// Summing without keeping the parts was an unforced loss: a cached prompt
	// and a cold one bill very differently, and a total alone cannot tell them
	// apart, so nothing could answer whether caching was working.
	CacheRead, CacheWrite int
	// UsageMismatch reports that the provider's own token totals did not add up,
	// which means this gateway's billing figure may be wrong. It is surfaced as a
	// metric rather than an error: the request itself is fine, but a provider
	// changing how it accounts for tokens must not silently drift revenue.
	UsageMismatch bool
}

// usageOf lifts the token counts out of a normalized response and into the
// shape telemetry exports. Deliberately a function beside the struct it reads
// rather than a method on the other side: a count added above has to be carried
// here, and the two field lists sitting together is what makes that obvious.
func usageOf(n normalized) tokenUsage {
	return tokenUsage{
		Input:      n.Input,
		Output:     n.Output,
		Reasoning:  n.Reasoning,
		CacheRead:  n.CacheRead,
		CacheWrite: n.CacheWrite,
	}
}

func finish(s string) (string, error) {
	switch s {
	case "stop", "end_turn", "stop_sequence", "STOP":
		return "stop", nil
	case "length", "max_tokens", "MAX_TOKENS":
		return "length", nil
	case "content_filter", "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII":
		return "content_filter", nil
	}
	return "", errors.New("unsupported finish reason")
}

type wire struct {
	Type    string          `json:"type"`
	Error   json.RawMessage `json:"error"`
	Choices []struct {
		Index   int `json:"index"`
		Message struct {
			Content   *string         `json:"content"`
			ToolCalls json.RawMessage `json:"tool_calls"`
			Refusal   *string         `json:"refusal"`
		} `json:"message"`
		Delta struct {
			Content   *string         `json:"content"`
			ToolCalls json.RawMessage `json:"tool_calls"`
			Refusal   *string         `json:"refusal"`
		} `json:"delta"`
		Finish *string `json:"finish_reason"`
	} `json:"choices"`
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StopReason string `json:"stop_reason"`
	Delta      struct {
		Type string `json:"type"`
		Text string `json:"text"`
		Stop string `json:"stop_reason"`
	} `json:"delta"`
	ContentBlock struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content_block"`
	Usage struct {
		Input  int `json:"input_tokens"`
		Output int `json:"output_tokens"`
		// Anthropic's prompt caching splits input three ways and input_tokens is
		// only one of them: "the tokens that come after the last cache breakpoint
		// in your request - not all the input tokens you sent". The three are
		// disjoint and total_input = input + cache_creation + cache_read.
		//
		// Reading input_tokens alone is therefore not a small under-count, it is
		// an unbounded one. Anthropic's own example: 100,000 tokens read from
		// cache plus a 50-token message reports input_tokens = 50. A gateway that
		// stops there tells the caller their request cost 50 input tokens when it
		// cost 100,050.
		//
		// Both are zero unless the caller uses cache_control, which is why this
		// was invisible rather than absent: the defect arrives with the first
		// cached prompt, not with a deployment.
		CacheCreation int `json:"cache_creation_input_tokens"`
		CacheRead     int `json:"cache_read_input_tokens"`
		Prompt        int `json:"prompt_tokens"`
		Completion    int `json:"completion_tokens"`
		// Reasoning models bill hidden reasoning inside completion_tokens and
		// break it out here. Without it there is no way to tell a short answer
		// from a budget entirely consumed before any answer was written.
		Details struct {
			Reasoning int `json:"reasoning_tokens"`
		} `json:"completion_tokens_details"`
	} `json:"usage"`
	Candidates []struct {
		Index   int `json:"index"`
		Content struct {
			Parts []struct {
				Text     string          `json:"text"`
				Thought  bool            `json:"thought"`
				Function json.RawMessage `json:"functionCall"`
				Inline   json.RawMessage `json:"inlineData"`
			} `json:"parts"`
		} `json:"content"`
		Finish string `json:"finishReason"`
	} `json:"candidates"`
	PromptFeedback struct {
		Block string `json:"blockReason"`
	} `json:"promptFeedback"`
	UsageMetadata struct {
		Input int `json:"promptTokenCount"`
		// Output is the visible answer only. Thinking models bill their internal
		// reasoning separately in thoughtsTokenCount, and may omit
		// candidatesTokenCount altogether, so this field alone under-reports
		// output badly: an observed call billed 8 prompt + 103 thought tokens and
		// reported candidatesTokenCount not at all.
		Output   int `json:"candidatesTokenCount"`
		Thoughts int `json:"thoughtsTokenCount"`
		Total    int `json:"totalTokenCount"`
	} `json:"usageMetadata"`
}

func normalize(provider string, b []byte, stream bool) (normalized, bool, error) {
	if provider == "bedrock" {
		if stream {
			// Frames here have already been translated out of AWS event-stream
			// framing by bedrockSSE, so they arrive as ordinary SSE payloads.
			return normalizeBedrockStream(b)
		}
		n, e := normalizeBedrock(b)
		return n, true, e
	}
	var w wire
	var n normalized
	if json.Unmarshal(b, &w) != nil {
		return n, false, errors.New("invalid provider JSON")
	}
	if len(w.Error) > 0 && string(w.Error) != "null" {
		return n, false, errors.New("provider stream error")
	}
	done := false
	switch provider {
	case "openai":
		n.Input = w.Usage.Prompt
		n.Output = w.Usage.Completion
		n.Reasoning = w.Usage.Details.Reasoning
		if len(w.Choices) > 1 {
			return n, false, errors.New("multiple choices unsupported")
		}
		for _, c := range w.Choices {
			if c.Index != 0 {
				return n, false, errors.New("invalid choice")
			}
			text := c.Message.Content
			tools := c.Message.ToolCalls
			refusal := c.Message.Refusal
			if stream {
				text = c.Delta.Content
				tools = c.Delta.ToolCalls
				refusal = c.Delta.Refusal
			}
			if len(tools) > 0 && string(tools) != "null" && string(tools) != "[]" {
				return n, false, errors.New("unexpected tool call")
			}
			if text != nil {
				n.Text = *text
			}
			if refusal != nil {
				n.Text += *refusal
			}
			if c.Finish != nil {
				f, e := finish(*c.Finish)
				if e != nil {
					return n, false, e
				}
				n.Finish = f
				done = true
			}
		}
	case "anthropic":
		// All three, because they are disjoint parts of one total. Absent fields
		// decode to zero, so an uncached response is unchanged.
		n.Input = w.Usage.Input + w.Usage.CacheCreation + w.Usage.CacheRead
		n.Output = w.Usage.Output
		// The parts as well as the sum. Anthropic is the only one of the four
		// that reports a cache split today, so these stay zero elsewhere, which
		// is the correct value rather than a missing one.
		n.CacheRead, n.CacheWrite = w.Usage.CacheRead, w.Usage.CacheCreation
		if stream {
			switch w.Type {
			case "content_block_start":
				if w.ContentBlock.Type != "text" {
					return n, false, errors.New("unsupported content block")
				}
				n.Text = w.ContentBlock.Text
			case "content_block_delta":
				if w.Delta.Type != "text_delta" {
					return n, false, errors.New("unsupported content delta")
				}
				n.Text = w.Delta.Text
			case "message_delta":
				if w.Delta.Stop != "" {
					f, e := finish(w.Delta.Stop)
					if e != nil {
						return n, false, e
					}
					n.Finish = f
				}
			case "message_stop":
				done = true
			case "message_start", "content_block_stop", "ping":
			default:
				return n, false, errors.New("unknown stream event")
			}
		} else {
			for _, c := range w.Content {
				if c.Type != "text" {
					return n, false, errors.New("unsupported content block")
				}
				n.Text += c.Text
			}
			f, e := finish(w.StopReason)
			if e != nil {
				return n, false, e
			}
			n.Finish = f
			done = true
		}
	case "gemini":
		n.Input = w.UsageMetadata.Input
		// Thinking tokens are billed as output, so they must be counted as output.
		n.Output = w.UsageMetadata.Output + w.UsageMetadata.Thoughts
		n.Reasoning = w.UsageMetadata.Thoughts
		// The provider also reports a total. When it disagrees with the parts,
		// this gateway is billing on an accounting model the provider no longer
		// uses, so say so rather than quietly trusting the sum.
		// Both guards matter: a stream frame carrying no usage at all reports
		// zeroes, and one carrying a total but no prompt count is partial. Neither
		// is a disagreement, so neither should be reported as one.
		if t := w.UsageMetadata.Total; t > 0 && w.UsageMetadata.Input > 0 &&
			t-w.UsageMetadata.Input != n.Output {
			n.UsageMismatch = true
		}
		if w.PromptFeedback.Block != "" {
			n.Finish = "content_filter"
			return n, true, nil
		}
		if len(w.Candidates) > 1 {
			return n, false, errors.New("multiple candidates unsupported")
		}
		for _, c := range w.Candidates {
			if c.Index != 0 {
				return n, false, errors.New("invalid candidate")
			}
			for _, p := range c.Content.Parts {
				if len(p.Function) > 0 || len(p.Inline) > 0 {
					return n, false, errors.New("unsupported Gemini content")
				}
				if !p.Thought {
					n.Text += p.Text
				}
			}
			if c.Finish != "" {
				f, e := finish(c.Finish)
				if e != nil {
					return n, false, e
				}
				n.Finish = f
				done = true
			}
		}
	}
	if !stream && !done {
		return n, false, errors.New("incomplete provider response")
	}
	return n, done, nil
}
func completion(id string, r Route, n normalized, created int64) any {
	return map[string]any{"id": "chatcmpl-" + id, "object": "chat.completion", "created": created, "model": r.Model, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": n.Text}, "finish_reason": n.Finish}}, "usage": map[string]int{"prompt_tokens": n.Input, "completion_tokens": n.Output, "total_tokens": n.Input + n.Output}}
}
func chunk(id string, r Route, n normalized, created int64) any {
	delta := map[string]any{}
	if n.Text != "" {
		delta["content"] = n.Text
	}
	var f any
	if n.Finish != "" {
		f = n.Finish
	}
	return map[string]any{"id": "chatcmpl-" + id, "object": "chat.completion.chunk", "created": created, "model": r.Model, "choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": f}}}
}

// SSE event assembly supports CRLF, comments and multi-line data. Bound both lines
// and assembled events so a malicious upstream cannot grow memory without limit.
func readSSE(r io.Reader, fn func([]byte) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var data []byte
	dispatch := func() error {
		if len(data) == 0 {
			return nil
		}
		e := fn(bytes.TrimSuffix(data, []byte("\n")))
		data = nil
		return e
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if e := dispatch(); e != nil {
				return e
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			v := strings.TrimPrefix(line, "data:")
			v = strings.TrimPrefix(v, " ")
			data = append(data, v...)
			data = append(data, '\n')
			if len(data) > 1<<20 {
				return errors.New("oversized SSE event")
			}
		}
	}
	if e := scanner.Err(); e != nil {
		return e
	}
	return dispatch()
}

var streamComplete = errors.New("complete")

func writeSSE(w http.ResponseWriter, v any) error {
	_, e := fmt.Fprintf(w, "data: %s\n\n", jsonBytes(v))
	if e == nil {
		e = http.NewResponseController(w).Flush()
	}
	return e
}
