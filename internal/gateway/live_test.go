package gateway

// Live provider verification.
//
// These call the real OpenAI, Anthropic and Gemini APIs. They are skipped
// unless explicitly enabled, because they cost money and need credentials.
//
//	SWITCHBOARD_LIVE_OPENAI=1    OPENAI_API_KEY=...    go test -run Live ./internal/gateway/
//	SWITCHBOARD_LIVE_ANTHROPIC=1 ANTHROPIC_API_KEY=... go test -run Live ./internal/gateway/
//	SWITCHBOARD_LIVE_GEMINI=1    GEMINI_API_KEY=...    go test -run Live ./internal/gateway/
//
// They exist because the equivalent test against real Bedrock immediately found
// a defect that would have failed every genuine request: the response carried a
// field the wire struct did not model, and decoding was strict. A mock cannot
// find that class of bug, because a mock only returns the fields we wrote into
// it. These three adapters carry roughly thirty response-shape assumptions that
// have only ever been checked against mocks built from the same assumptions.
//
// The request goes through upstream() and the response through normalize(),
// which is the exact path a production request takes, so request construction,
// authentication, response parsing and finish-reason mapping are all exercised
// together rather than in isolation.

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

type liveProvider struct {
	name, gate, keyEnv, url, model string
	// truncHTTP400 records that this provider signals budget exhaustion as an
	// HTTP 400 rather than in band as a finish reason. OpenAI's reasoning models
	// do; its legacy chat models do not. The gateway maps that 400 to "provider
	// rejected request" with no failover, so the distinction is not cosmetic.
	truncHTTP400 bool
}

// Model names are a live dependency, not a constant: gemini-2.0-flash and
// claude-3-5-haiku-latest were both retired out from under this table and
// returned 404 from the real APIs. Two OpenAI entries are deliberate, because the
// legacy and reasoning model families take different request shapes.
var liveProviders = []liveProvider{
	{"openai", "SWITCHBOARD_LIVE_OPENAI", "OPENAI_API_KEY",
		"https://api.openai.com", "gpt-4o-mini", false},
	{"openai-reasoning", "SWITCHBOARD_LIVE_OPENAI", "OPENAI_API_KEY",
		"https://api.openai.com", "gpt-5-nano", true},
	{"anthropic", "SWITCHBOARD_LIVE_ANTHROPIC", "ANTHROPIC_API_KEY",
		"https://api.anthropic.com", "claude-haiku-4-5-20251001", false},
	{"gemini", "SWITCHBOARD_LIVE_GEMINI", "GEMINI_API_KEY",
		"https://generativelanguage.googleapis.com", "gemini-3.6-flash", false},
}

// adapter is the provider name the gateway routes on. The reasoning entry
// exercises a different model through the same openai adapter.
func (p liveProvider) adapter() string {
	if p.name == "openai-reasoning" {
		return "openai"
	}
	return p.name
}

// liveRetries bounds retries against a busy provider. Three attempts is enough
// to ride out a brief capacity spike without turning a broken adapter into a
// slow test.
const liveRetries = 3

func (p liveProvider) skipUnlessEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv(p.gate) != "1" {
		t.Skipf("set %s=1 and %s to exercise the real %s API", p.gate, p.keyEnv, p.name)
	}
	if os.Getenv(p.keyEnv) == "" {
		t.Fatalf("%s is set but %s is empty", p.gate, p.keyEnv)
	}
}

func (p liveProvider) send(t *testing.T, c Chat) *http.Response {
	t.Helper()
	// Each attempt gets its own deadline. Sharing one across retries meant a
	// provider that was slow to say "busy" burned the whole budget before the
	// retry could run, which surfaced as a timeout rather than as the 503 it was.
	const perAttempt = 45 * time.Second
	var res *http.Response
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), perAttempt)
		t.Cleanup(cancel)
		req, err := upstream(ctx, c,
			Route{Provider: p.adapter(), Model: p.model},
			ProviderConfig{URL: p.url, KeyEnv: p.keyEnv}, nil)
		if err != nil {
			t.Fatalf("%s: could not build the request: %v", p.name, err)
		}
		res, err = http.DefaultClient.Do(req)
		if err == nil {
			t.Cleanup(func() { res.Body.Close() })
			// 503 (busy) and 429 (throttled) both say the provider would not
			// serve this request right now, not that the adapter is wrong.
			// Retrying keeps the test measuring what it is meant to measure.
			// Gemini in particular enforces a short per-minute quota.
			//
			// But not every 429 is capacity. classify() already distinguishes a
			// rate limit from an account that cannot serve at all -- no credits,
			// quota exhausted, balance too low -- and that second kind never
			// clears by waiting. Retried three times and then skipped, it
			// reported the suite green while every request was failing for
			// billing reasons, which is exactly what this file refuses to do a
			// hundred lines below: "Not Skip: skipping here reported the parent
			// test as PASS when every request was failing authentication."
			if res.StatusCode == 429 {
				body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
				res.Body.Close()
				if accountExhausted(body) {
					t.Fatalf("%s: the account cannot serve: %s\n"+
						"This is a billing state, not provider capacity. Waiting will not clear it.",
						p.name, providerReason(body))
				}
			} else if res.StatusCode != 503 {
				return res
			} else {
				res.Body.Close()
			}
		}
		if attempt == liveRetries {
			if err != nil {
				t.Fatalf("%s: request failed after %d attempts: %v", p.name, attempt, err)
			}
			t.Skipf("%s: provider was busy or throttled on all %d attempts "+
				"(last status %d); this is provider capacity, not an adapter defect",
				p.name, attempt, res.StatusCode)
		}
		t.Logf("%s: attempt %d did not succeed (err=%v), retrying", p.name, attempt, err)
		time.Sleep(time.Duration(attempt) * 4 * time.Second)
	}
}

// A complete response, through the same normalize() a production request uses.
func TestLiveNonStreaming(t *testing.T) {
	for _, p := range liveProviders {
		t.Run(p.name, func(t *testing.T) {
			p.skipUnlessEnabled(t)
			res := p.send(t, Chat{
				// 512, not 16: a reasoning model spends its whole allowance on
				// internal thinking first and returns no visible text at all
				// below roughly this budget.
				MaxTokens: 512,
				Messages:  []Message{{Role: "user", Content: "Reply with the single word: ok"}},
			})
			body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
			if res.StatusCode != 200 {
				t.Fatalf("%s returned %d: %s", p.name, res.StatusCode, truncate(body))
			}
			n, _, err := normalize(p.adapter(), body, false)
			if err != nil {
				// The Bedrock equivalent of this failure was a real defect, not
				// a test problem, so the body is printed to make it diagnosable.
				t.Fatalf("%s: a real response did not normalize: %v\nbody: %s",
					p.name, err, truncate(body))
			}
			if n.Text == "" {
				t.Errorf("%s: normalized to empty text", p.name)
			}
			if n.Finish == "" {
				t.Errorf("%s: no finish reason", p.name)
			}
			if n.Input == 0 && n.Output == 0 {
				// Not fatal: usage accounting differs per provider and may be
				// absent. Worth surfacing because billing would depend on it.
				t.Logf("%s: no token usage reported", p.name)
			}
			// Bedrock's Converse response names no model; every other provider does,
			// and gen_ai.response.model is only as good as this.
			if n.Model == "" && p.adapter() != "bedrock" {
				t.Errorf("%s: a real response named no served model", p.name)
			}
			t.Logf("%s: text=%q finish=%s in=%d out=%d served=%q", p.name, n.Text, n.Finish, n.Input, n.Output, n.Model)
		})
	}
}

// Streaming is where truncation handling and the finish-reason rules live, and
// where a hand-written mock proves least: real providers chunk differently,
// send keepalives, and terminate in their own ways.
func TestLiveStreaming(t *testing.T) {
	for _, p := range liveProviders {
		t.Run(p.name, func(t *testing.T) {
			p.skipUnlessEnabled(t)
			res := p.send(t, Chat{
				Stream:    true,
				MaxTokens: 512,
				Messages:  []Message{{Role: "user", Content: "Count: one two three"}},
			})
			if res.StatusCode != 200 {
				body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
				t.Fatalf("%s returned %d: %s", p.name, res.StatusCode, truncate(body))
			}
			if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "event-stream") {
				t.Errorf("%s: streaming content type is %q", p.name, ct)
			}

			var text strings.Builder
			var served string
			var finish string
			frames, done := 0, false
			sc := bufio.NewScanner(res.Body)
			sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
			for sc.Scan() {
				line := strings.TrimSpace(sc.Text())
				if !strings.HasPrefix(line, "data:") {
					continue
				}
				payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				if payload == "[DONE]" {
					done = true
					break
				}
				frames++
				n, complete, err := normalize(p.adapter(), []byte(payload), true)
				if err != nil {
					t.Fatalf("%s: a real stream frame did not normalize: %v\nframe: %s",
						p.name, err, truncate([]byte(payload)))
				}
				text.WriteString(n.Text)
				if n.Model != "" {
					served = n.Model
				}
				if n.Finish != "" {
					finish = n.Finish
				}
				if complete {
					done = true
					break
				}
			}
			if err := sc.Err(); err != nil {
				t.Fatalf("%s: reading the stream failed: %v", p.name, err)
			}
			if frames == 0 {
				t.Fatalf("%s: no data frames", p.name)
			}
			if !done {
				// The gateway treats a stream ending without terminal marker as
				// truncation, so if a real provider does this the rule is wrong.
				t.Errorf("%s: stream ended without a terminal marker", p.name)
			}
			if finish == "" {
				t.Errorf("%s: stream carried no finish reason", p.name)
			}
			// Settles whether Gemini states modelVersion on stream chunks, and that
			// Anthropic's message_start reaches normalize as a data frame.
			if served == "" && p.adapter() != "bedrock" {
				t.Errorf("%s: no stream frame named a served model", p.name)
			}
			t.Logf("%s: %d frames, finish=%s, text=%q, served=%q", p.name, frames, finish, text.String(), served)
		})
	}
}

// The gateway rejects a response carrying a finish reason it does not model,
// so an unmapped value is a hard failure rather than a degraded one. This
// records what each provider actually sends when output is cut short.
func TestLiveTruncatedFinishReason(t *testing.T) {
	for _, p := range liveProviders {
		t.Run(p.name, func(t *testing.T) {
			p.skipUnlessEnabled(t)
			res := p.send(t, Chat{
				MaxTokens: 1, // forces the length/max_tokens path
				Messages:  []Message{{Role: "user", Content: "Write a long paragraph about the sea."}},
			})
			body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
			if p.truncHTTP400 {
				// Recorded, not tolerated: this provider reports "output was cut
				// short" as a client error, so the gateway surfaces a bare 400
				// and does not fail over. See docs/GAPS.md.
				if res.StatusCode != 400 {
					t.Fatalf("%s: expected truncation to surface as HTTP 400, got %d: %s",
						p.name, res.StatusCode, truncate(body))
				}
				t.Logf("%s: truncation signalled as HTTP 400, not a finish reason: %s",
					p.name, truncate(body))
				return
			}
			if res.StatusCode != 200 {
				// Not Skip: skipping here reported the parent test as PASS when
				// every request was failing authentication.
				t.Fatalf("%s returned %d for a one-token request: %s",
					p.name, res.StatusCode, truncate(body))
			}
			n, _, err := normalize(p.adapter(), body, false)
			if err != nil {
				t.Fatalf("%s: truncated response did not normalize: %v\nbody: %s",
					p.name, err, truncate(body))
			}
			if n.Finish != "length" {
				t.Errorf("%s: expected finish=length when truncated, got %q", p.name, n.Finish)
			}
			t.Logf("%s: truncated finish=%s", p.name, n.Finish)
		})
	}
}

func truncate(b []byte) string {
	const max = 600
	if len(b) > max {
		return string(bytes.TrimSpace(b[:max])) + "…"
	}
	return string(bytes.TrimSpace(b))
}
