package gateway

// Amazon Bedrock support.
//
// Bedrock differs from the other three providers in two ways that matter to the
// shape of this package.
//
// Authentication is SigV4 against the task's IAM role rather than a static key
// from the environment, so there is no long-lived credential to leak and nothing
// for a customer to paste. That is a better story for a security product, but it
// means a request cannot be built without resolving credentials first.
//
// Streaming is AWS event-stream framing, not server-sent events. The other three
// adapters read `data:` lines; Bedrock returns length-prefixed binary messages
// with CRCs. Rather than reach for the bedrockruntime SDK client, which would
// bypass this package's transport entirely and with it the circuit breaker, the
// retry budget, the no-replay-after-acceptance rule and the generation deadline,
// this signs an ordinary *http.Request and decodes the *http.Response body. Every
// safety property already built here continues to apply.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// streams reports whether a provider can serve a streaming request. All four do:
// Bedrock's AWS event-stream framing is translated into server-sent events at
// the provider boundary by bedrockSSE, so the rest of the pipeline sees one wire
// format and the no-replay-after-acceptance rule applies unchanged.
func streams(string) bool { return true }

// BedrockSigner signs outbound Bedrock requests. It is nil unless a bedrock
// provider is configured, which keeps the common case free of AWS credential
// resolution entirely.
type BedrockSigner struct {
	signer *v4.Signer
	creds  aws.CredentialsProvider
	region string
}

// NewBedrockSigner resolves the ambient AWS configuration once, at startup.
// Credentials are cached and refreshed by the provider, so this is not repeated
// per request.
func NewBedrockSigner(ctx context.Context, region string) (*BedrockSigner, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("aws configuration unavailable for bedrock: %w", err)
	}
	if cfg.Credentials == nil {
		return nil, errors.New("no AWS credentials available for bedrock")
	}
	return &BedrockSigner{signer: v4.NewSigner(), creds: cfg.Credentials, region: region}, nil
}

// sign applies SigV4 in place. The payload hash is required by the algorithm, so
// the body must already be set.
func (b *BedrockSigner) sign(ctx context.Context, req *http.Request, body []byte) error {
	c, err := b.creds.Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("bedrock credentials unavailable: %w", err)
	}
	sum := sha256.Sum256(body)
	return b.signer.SignHTTP(ctx, c, req, hex.EncodeToString(sum[:]),
		"bedrock", b.region, time.Now().UTC())
}

// bedrockBody builds a Converse request. maxTokens is always set: leaving it
// unset makes Bedrock reserve the model's maximum, which silently consumes far
// more quota than the request needs and is a common cause of throttling.
func bedrockBody(c Chat, system string, msgs []Message) map[string]any {
	content := func(text string) []any { return []any{map[string]any{"text": text}} }
	out := make([]any, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, map[string]any{"role": m.Role, "content": content(m.Content)})
	}
	inference := map[string]any{"maxTokens": c.MaxTokens}
	if c.Temperature != nil {
		inference["temperature"] = *c.Temperature
	}
	b := map[string]any{"messages": out, "inferenceConfig": inference}
	if system != "" {
		b["system"] = content(system)
	}
	return b
}

// bedrockWire is the subset of a Converse response this gateway uses. Tool and
// multimodal blocks are deliberately absent: they are unsupported here, and a
// response carrying them is rejected rather than silently flattened.
type bedrockWire struct {
	Output struct {
		Message struct {
			Content []struct {
				Text      string          `json:"text"`
				ToolUse   json.RawMessage `json:"toolUse"`
				Reasoning json.RawMessage `json:"reasoningContent"`
			} `json:"content"`
		} `json:"message"`
	} `json:"output"`
	StopReason string `json:"stopReason"`
	Usage      struct {
		Input  int `json:"inputTokens"`
		Output int `json:"outputTokens"`
	} `json:"usage"`
	// Streaming deltas arrive as separate event shapes on the same connection.
	Delta struct {
		Text string `json:"text"`
	} `json:"delta"`
	ContentBlockIndex *int `json:"contentBlockIndex"`
	// No model field, and not an omission. The Converse response names none: its
	// members are output, stopReason, usage, metrics, trace, performanceConfig,
	// serviceTier and additionalModelResponseFields. A Bedrock attempt therefore
	// never has a served model, and gen_ai.response.model is absent from its span
	// rather than filled in with the model that was requested. See docs/GAPS.md.
}

// bedrockFinish maps Converse stopReason onto the gateway's vocabulary. Values
// not listed are rejected rather than guessed at, because a new stop reason is
// more likely to mean "something happened we do not model" than "stop".
func bedrockFinish(s string) (string, error) {
	switch s {
	case "end_turn", "stop_sequence":
		return "stop", nil
	case "max_tokens":
		return "length", nil
	case "guardrail_intervened", "content_filtered":
		return "content_filter", nil
	}
	return "", fmt.Errorf("unsupported bedrock stop reason %q", s)
}

// decodeBedrockStream reads AWS event-stream framing and returns the assembled
// text, the finish reason and token usage. It exists so the caller can treat a
// Bedrock stream like any other, and it enforces the same refusal of tool and
// reasoning blocks that the nonstreaming path does.
func decodeBedrockStream(r io.Reader, limit int64) (normalized, error) {
	var n normalized
	dec := eventstream.NewDecoder()
	var text bytes.Buffer
	payload := make([]byte, 0, 8192)
	for {
		msg, err := dec.Decode(io.LimitReader(r, limit), payload)
		if err == io.EOF {
			break
		}
		if err != nil {
			return n, fmt.Errorf("malformed bedrock event stream: %w", err)
		}
		// The event name lives in the message headers; the body is JSON.
		var kind string
		for _, h := range msg.Headers {
			if h.Name == ":event-type" {
				kind = h.Value.String()
			}
		}
		var w bedrockWire
		if len(msg.Payload) > 0 {
			if err := json.Unmarshal(msg.Payload, &w); err != nil {
				return n, fmt.Errorf("malformed bedrock event payload: %w", err)
			}
		}
		switch kind {
		case "contentBlockDelta":
			if len(w.Delta.Text) > 0 {
				text.WriteString(w.Delta.Text)
			}
		case "messageStop":
			f, err := bedrockFinish(w.StopReason)
			if err != nil {
				return n, err
			}
			n.Finish = f
		case "metadata":
			n.Input, n.Output = w.Usage.Input, w.Usage.Output
		}
		if text.Len() > int(limit) {
			return n, errors.New("bedrock stream exceeded the response limit")
		}
	}
	if n.Finish == "" {
		return n, errors.New("bedrock stream ended without a stop reason")
	}
	n.Text = text.String()
	return n, nil
}

// bedrockSSE translates AWS event-stream framing into the server-sent events the
// rest of this package reads, so streaming Bedrock costs no new machinery in
// stream(): the deadline, the finish tracking, the empty-completion detection and
// the refusal to replay after acceptance all keep working as written.
//
// It does one thing beyond translation, and it matters. Bedrock ends a stream
// with messageStop carrying the stop reason and then metadata carrying token
// usage. The pipeline terminates on the first frame that reports completion, so
// emitting messageStop as it arrives would end the stream before usage was ever
// seen and report every streamed Bedrock request as costing nothing. The stop
// reason is therefore held back and emitted together with usage as one terminal
// frame.
func bedrockSSE(body io.ReadCloser, limit int64) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		defer body.Close()
		pw.CloseWithError(translateBedrockStream(body, pw, limit))
	}()
	return pr
}

// frames the translator emits are ordinary Converse field shapes rather than an
// invented envelope, so normalize can decode them with the same bedrockWire it
// already uses: a delta carries delta.text, and the terminal frame carries
// stopReason and usage.
func translateBedrockStream(r io.Reader, w io.Writer, limit int64) error {
	dec := eventstream.NewDecoder()
	payload := make([]byte, 0, 8192)
	var stop string
	var usage []byte
	written := int64(0)
	emit := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(w, "data: %s\n\n", b)
		return err
	}
	for {
		msg, err := dec.Decode(io.LimitReader(r, limit), payload)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("malformed bedrock event stream: %w", err)
		}
		var kind string
		for _, h := range msg.Headers {
			if h.Name == ":event-type" {
				kind = h.Value.String()
			}
		}
		var e bedrockWire
		if len(msg.Payload) > 0 {
			if err := json.Unmarshal(msg.Payload, &e); err != nil {
				return fmt.Errorf("malformed bedrock event payload: %w", err)
			}
		}
		switch kind {
		case "contentBlockStart":
			// Tools and reasoning are unsupported here, exactly as in the
			// nonstreaming path. Flattening them to text would discard content
			// the caller never agreed to lose.
			if bytes.Contains(msg.Payload, []byte(`"toolUse"`)) ||
				bytes.Contains(msg.Payload, []byte(`"reasoningContent"`)) {
				return errors.New("bedrock returned an unsupported content block")
			}
		case "contentBlockDelta":
			if e.Delta.Text == "" {
				continue
			}
			written += int64(len(e.Delta.Text))
			if written > limit {
				return errors.New("bedrock stream exceeded the response limit")
			}
			if err := emit(map[string]any{"delta": map[string]any{"text": e.Delta.Text}}); err != nil {
				return err
			}
		case "messageStop":
			// Held, not emitted: see the note above.
			stop = e.StopReason
		case "metadata":
			// Cloned, not retained. The vendored eventstream decoder builds each
			// payload over the caller's buffer, so holding this slice means the
			// next Decode overwrites the token counts in place.
			//
			// That is benign only because Bedrock sends metadata last and no
			// later decode succeeds -- a property of AWS's current emission
			// order, not of this code. The day anything follows metadata, or it
			// moves ahead of messageStop, the caller is handed wrong token counts
			// with no error: the numbers still parse, so UsageMismatch does not
			// fire and nothing downstream can tell. One allocation, once per
			// stream, on the terminal frame.
			usage = bytes.Clone(msg.Payload)
		}
	}
	if stop == "" {
		return errors.New("bedrock stream ended without a stop reason")
	}
	final := map[string]any{"stopReason": stop}
	if len(usage) > 0 {
		var u struct {
			Usage json.RawMessage `json:"usage"`
		}
		if json.Unmarshal(usage, &u) == nil && len(u.Usage) > 0 {
			final["usage"] = json.RawMessage(u.Usage)
		}
	}
	return emit(final)
}

// normalizeBedrockStream reads one translated frame. Completion is signalled by
// the terminal frame carrying a stop reason, which is also the frame carrying
// usage, so a caller that stops at the first completion still gets token counts.
func normalizeBedrockStream(b []byte) (normalized, bool, error) {
	var n normalized
	var w bedrockWire
	if err := json.Unmarshal(b, &w); err != nil {
		return n, false, err
	}
	n.Text = w.Delta.Text
	n.Input, n.Output = w.Usage.Input, w.Usage.Output
	if w.StopReason == "" {
		return n, false, nil
	}
	f, err := bedrockFinish(w.StopReason)
	if err != nil {
		return n, false, err
	}
	n.Finish = f
	return n, true, nil
}

// normalizeBedrock handles a complete (nonstreaming) Converse response.
func normalizeBedrock(b []byte) (normalized, error) {
	var n normalized
	// Lenient, matching the other three adapters. Strict decoding belongs on
	// client input, where an unknown field means the caller asked for something
	// unsupported; a provider adding a response field is routine, and rejecting
	// it would break every request. Live Bedrock returns a "metrics" object this
	// gateway has no use for, which is exactly the case in point. The unsupported
	// content blocks are refused explicitly below rather than by strictness.
	var w bedrockWire
	if err := json.Unmarshal(b, &w); err != nil {
		return n, err
	}
	for _, block := range w.Output.Message.Content {
		if len(block.ToolUse) > 0 || len(block.Reasoning) > 0 {
			return n, errors.New("bedrock returned an unsupported content block")
		}
		n.Text += block.Text
	}
	f, err := bedrockFinish(w.StopReason)
	if err != nil {
		return n, err
	}
	n.Finish = f
	n.Input, n.Output = w.Usage.Input, w.Usage.Output
	return n, nil
}
