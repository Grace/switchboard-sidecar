package gateway

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestExpiredCacheRetainsVersionAndCanRecover(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	s := &PolicyStore{Tenant: "tenant-a", Keys: map[string]ed25519.PublicKey{"k": pub}}
	p := testPolicy()
	p.Version = 10
	p.IssuedAt = time.Now().Unix() - 100
	p.ExpiresAt = time.Now().Unix() - 1
	if e := s.Restore(signed(t, p, key, "k")); e != nil {
		t.Fatal(e)
	}
	if s.Current() != nil {
		t.Fatal("expired policy active")
	}
	p = testPolicy()
	p.Version = 9
	if s.Apply(signed(t, p, key, "k"), false) == nil {
		t.Fatal("lost high-water mark")
	}
	p.Version = 11
	if e := s.Apply(signed(t, p, key, "k"), false); e != nil || s.Current() == nil {
		t.Fatal(e)
	}
}

type failingTransport struct{ calls int }

func (f *failingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.calls++
	return nil, errors.New("ambiguous failure")
}
func TestTransportAmbiguityNotReplayed(t *testing.T) {
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: "https://api.openai.com", KeyEnv: "PROVIDER_KEY"}, "anthropic": {URL: "https://api.anthropic.com", KeyEnv: "PROVIDER_KEY"}})
	f := &failingTransport{}
	s.HTTP.Transport = f
	if call(s, chat).Code != 502 || f.calls != 1 {
		t.Fatal("ambiguous request replayed")
	}
}
func TestCancelledRequestNotReplayed(t *testing.T) {
	s := testServer(t, map[string]ProviderConfig{"openai": {URL: "https://api.openai.com", KeyEnv: "PROVIDER_KEY"}})
	f := &failingTransport{}
	s.HTTP.Transport = f
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(chat)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 502 || s.circuits["openai"].failures != 0 {
		t.Fatal("cancellation penalized provider")
	}
}

// The Prometheus surface renders the same keyed data as OTLP, under the names
// OpenTelemetry's Prometheus mapping derives: dots to underscores, the UCUM unit
// converted to a word and appended. Buckets here are cumulative, which is what
// we store and the opposite of what OTLP wants.
func TestMetricsHistogram(t *testing.T) {
	m := &Metrics{}
	m.ObserveRoute("openai", "gpt-5-nano", 200, 300, tokenUsage{})
	m.ObserveRoute("openai", "gpt-5-nano", 200, 800, tokenUsage{})
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	l := `{gen_ai_operation_name="chat",gen_ai_provider_name="openai",gen_ai_request_model="gpt-5-nano"`
	for _, want := range []string{
		"# TYPE gen_ai_server_request_duration_seconds histogram",
		// 300ms is at or below 0.5s and 800ms is not: cumulative, so one here.
		`gen_ai_server_request_duration_seconds_bucket` + l + `,le="0.5"} 1`,
		// Both are at or below 1s.
		`gen_ai_server_request_duration_seconds_bucket` + l + `,le="1"} 2`,
		`gen_ai_server_request_duration_seconds_bucket` + l + `,le="+Inf"} 2`,
		// Seconds, and carrying the same labels as the buckets.
		`gen_ai_server_request_duration_seconds_sum` + l + `} 1.1`,
		`gen_ai_server_request_duration_seconds_count` + l + `} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
	// No usage was reported, so the token metric must not appear at all.
	if strings.Contains(body, "gen_ai_client_token_usage") {
		t.Errorf("token metric emitted with no tokens counted:\n%s", body)
	}
}
