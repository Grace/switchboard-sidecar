package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	Requests, Errors, Retries, Rejected, Dropped, DiskErrors, ExportErrors, PolicyErrors, SpoolUsed, SpoolCount, Active, LatencyMS, Completed atomic.Int64
	// RateLimited counts provider 429s, which are deliberately not Errors:
	// the provider was healthy and the request failed over.
	RateLimited atomic.Int64
	// UsageMismatch counts responses whose own token totals did not add up. It is
	// a billing-integrity signal: a provider changing how it accounts for tokens
	// shows up here rather than as silent revenue drift.
	UsageMismatch atomic.Int64
	// EmptyCompletion counts responses that succeeded and carried no text. A
	// reasoning model can spend an entire token budget on hidden reasoning and
	// return nothing, billed in full, which would otherwise look like success.
	//
	// This is counted PER ROUTE, so one request can advance it more than once.
	// The two counters below are per request, and are what an alarm should use:
	// this one cannot distinguish a request that recovered from one that failed,
	// which is the distinction an operator actually needs.
	EmptyCompletion atomic.Int64
	// EmptyCompletionRecovered: a later route answered. The caller got a real
	// response, but paid two providers and waited for both. Wasted spend and
	// latency rather than lost service, so it warrants a higher alarm threshold.
	EmptyCompletionRecovered atomic.Int64
	// EmptyCompletionFailed: every attempted route came back empty and the caller
	// got a 503. This is lost service and warrants a low threshold.
	EmptyCompletionFailed atomic.Int64
	// AccountFailover counts requests moved to another provider because an
	// account could not serve at all: no credits, balance too low, quota gone.
	// Distinct from RateLimited, which is transient and self-clearing.
	AccountFailover atomic.Int64
	// RefusedFailover counts requests moved to another provider because this one
	// refused before generating: 401, 403 or 404. Counted apart from
	// AccountFailover because the operator action differs -- an account failover
	// means pay the provider, a refusal means fix a credential or a model name --
	// and apart from Errors because the caller was served.
	RefusedFailover atomic.Int64
	// ProviderProbeFailed counts providers rejected by the startup check.
	ProviderProbeFailed atomic.Int64
	// IdempotentReplay counts responses served from the idempotency store rather
	// than from a provider. Every one is a generation not paid for twice.
	IdempotentReplay atomic.Int64
	// IdempotentConflict counts keys refused because they were still in use, or
	// reused with a different request body.
	IdempotentConflict atomic.Int64
	// BudgetSkip counts routes passed over because that model was already seen
	// returning nothing at the caller's token budget. Every one is a provider
	// call not made and not billed.
	BudgetSkip atomic.Int64
	// IdempotentUnknown counts retries refused because the original outcome is
	// genuinely unknown. A rising figure here means real ambiguity is being
	// caught rather than silently paid for twice.
	IdempotentUnknown atomic.Int64
	// Goroutines, HeapAlloc, HeapObjects and HeapSys are sampled from the
	// runtime rather than counted, and exist to make unbounded growth
	// attributable. A 30-minute soak found resident memory rising linearly with
	// requests served and not falling when load stopped; these four separate the
	// three explanations that observation leaves open.
	//
	// Goroutines rising with requests is a goroutine leak and needs nothing
	// further. HeapObjects and HeapAlloc rising together is object retention.
	// HeapSys rising while HeapAlloc stays flat is fragmentation or memory the
	// runtime has kept rather than objects the program is holding, which a heap
	// profile would not explain.
	Goroutines, HeapAlloc, HeapObjects, HeapSys atomic.Int64
	// PolicyExpiresIn is seconds until the live policy expires, sampled rather
	// than counted. Negative once it has.
	//
	// A policy lasts at most seven days (controlplane/policy.py enforces 604800,
	// policy.go verifies the same bound), renewal is manual, and docs/SECURITY.md
	// says so. Nothing measured the remaining time, so the first signal a
	// deployment got was /readyz turning 503 -- after it had already stopped
	// serving. The value only reaches an operator if otlp_metrics_url is set;
	// /metrics is loopback-only, which is what UncollectedMetricsWarning is for.
	PolicyExpiresIn atomic.Int64
	// TemperatureDropped counts requests sent without the caller's temperature
	// because the model refuses it. Silently changing a caller's parameters is
	// worse than the 400 it replaces unless it is visible, which is the same
	// argument as usage_mismatch_total.
	TemperatureDropped atomic.Int64
	// CaptureFull and IdemFull separate a capacity decision from an I/O failure.
	// One DiskErrors counter covered the spool, the idempotency store and the
	// capture store, and covered "the disk refused" and "this store hit its own
	// budget" alike -- so a rising number told an operator neither which store
	// was in trouble nor whether raising a limit would fix it.
	CaptureFull, IdemFull atomic.Int64
	LatencyBuckets        [len(latencyBounds)]atomic.Int64
	LogDropped            *atomic.Int64
	// RouteKeysFolded counts observations that arrived after the route table was
	// full and were recorded without their model. See routeTable.
	RouteKeysFolded atomic.Int64
	routes          routeTable
}

// The conventions' request-duration and token-usage metrics both require
// attributes that a single global counter cannot carry: at minimum the provider
// and the operation, and for tokens whether the count is input or output. Every
// metric this gateway exported was one undimensioned number, so there was no
// per-provider cost, latency or failure reading anywhere -- and no honest way to
// name one of them gen_ai.*, because a conformant name over data missing the
// attributes that name requires is worse than an obviously custom one.
//
// Keyed here rather than by promoting the flat counters, which are read on paths
// that have no route in hand and are a stable query surface of their own.
type routeKey struct {
	provider, model string
	// errorType is "" on success. It is the conventions' error.type, which is
	// Conditionally Required on the duration metric when the operation failed.
	errorType string
}

type routeLatency struct {
	buckets [len(latencyBounds)]atomic.Int64
	sumMS   atomic.Int64
	count   atomic.Int64
	// Token totals, kept on the same key so one lookup serves both metrics.
	// Split by the conventions' gen_ai.token.type at export rather than stored
	// twice.
	inputTokens, outputTokens atomic.Int64
}

// routeMaxKeys bounds the number of keys that carry a model. Models arrive from
// signed policies, which change over the life of a process, so that part of the
// key space has no bound of its own and an unbounded metric map is a memory leak
// with a slow fuse.
//
// The table can hold a few more than this: once the cap is reached, further
// models fold onto a per-provider key, and those are bounded separately and much
// more tightly -- provider is a closed four-value set that policy.go enforces,
// and error.type comes from the fixed set of statuses fail() uses. The total is
// therefore routeMaxKeys plus a small constant, not routeMaxKeys exactly.
const routeMaxKeys = 128

type routeTable struct {
	mu sync.RWMutex
	m  map[routeKey]*routeLatency
}

// get returns the aggregate for a route, creating it if there is room.
//
// Past the cap the model is dropped and the observation is folded onto the
// provider, rather than discarded. Losing one dimension is much better than
// losing the measurement: a fleet that trips this still reports correct totals
// per provider, and RouteKeysFolded says the detail is missing rather than
// leaving a silent hole. Provider is a closed four-value set that policy.go
// enforces, so the folded key cannot itself grow without bound.
func (t *routeTable) get(k routeKey, folded *atomic.Int64) *routeLatency {
	t.mu.RLock()
	r, ok := t.m[k]
	t.mu.RUnlock()
	if ok {
		return r
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// Re-check: another goroutine may have created it between the two locks.
	if r, ok := t.m[k]; ok {
		return r
	}
	if t.m == nil {
		t.m = map[routeKey]*routeLatency{}
	}
	if len(t.m) >= routeMaxKeys && k.model != "" {
		folded.Add(1)
		k.model = ""
		if r, ok := t.m[k]; ok {
			return r
		}
	}
	r = &routeLatency{}
	t.m[k] = r
	return r
}

// snapshot copies the table for export. Values are read under the lock but are
// individually atomic, so this is a consistent set of keys rather than a
// consistent instant -- which is what a cumulative histogram needs.
func (t *routeTable) snapshot() map[routeKey]*routeLatency {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[routeKey]*routeLatency, len(t.m))
	for k, v := range t.m {
		out[k] = v
	}
	return out
}

// ObserveRoute records one completed inference against its route.
//
// Called beside ObserveLatency rather than replacing it: the flat histogram
// still backs the Prometheus endpoint, and the duplicated bucket walk costs
// nine comparisons on a path that just spent hundreds of milliseconds talking
// to a provider.
func (m *Metrics) ObserveRoute(provider, model string, status int, ms int64, u tokenUsage) {
	k := routeKey{provider: provider, model: model}
	// Matches the span: error.type is the status code, which is the honest
	// taxonomy here because every fail() picks a distinct status for a distinct
	// cause.
	if status >= 400 {
		k.errorType = strconv.Itoa(status)
	}
	r := m.routes.get(k, &m.RouteKeysFolded)
	r.sumMS.Add(ms)
	r.count.Add(1)
	for i, b := range latencyBounds {
		if ms <= b {
			r.buckets[i].Add(1)
		}
	}
	r.inputTokens.Add(int64(u.Input))
	r.outputTokens.Add(int64(u.Output))
}

// The first bucket of an explicit-bounds histogram has no lower bound, so
// whatever sits below latencyBounds[0] is unmeasurable: a percentile drawn from
// that bucket extrapolates below zero and Honeycomb duly reports a negative
// duration. ObserveLatency runs from a defer in Server.chat on every request,
// not only on inference, so 5 and 25 are not padding. Auth rejections, policy
// errors, refused requests and idempotent replays all finish in single-digit
// milliseconds, and with a floor of 100 they were indistinguishable from each
// other and from a fast completion. Everything from 100 up is unchanged, so a
// query written against le="100" or above still means what it did.
var latencyBounds = [9]int64{5, 25, 100, 500, 1000, 5000, 15000, 60000, 90000}

func (m *Metrics) ObserveLatency(ms int64) {
	m.LatencyMS.Add(ms)
	m.Completed.Add(1)
	for i, b := range latencyBounds {
		if ms <= b {
			m.LatencyBuckets[i].Add(1)
		}
	}
}

// series is one exported metric. Counters and gauges are listed once, here, so
// the Prometheus endpoint and the OTLP exporter cannot drift apart: a counter
// added to one and forgotten in the other is invisible in exactly the place
// someone is looking for it.
type series struct {
	name, kind string
	v          *atomic.Int64
}

// sampleRuntime refreshes the four runtime gauges. ReadMemStats stops the
// world, which is normally the argument against calling it, but series() is
// read only by a Prometheus scrape and by the exporter every metricInterval, so
// the pause is tens of microseconds a couple of times a minute. That reasoning
// depends on the heap staying small: the pause scales with heap size, and a
// heap large enough to make this expensive is one these gauges should have
// caught long before. runtime/metrics is the non-stop-the-world alternative if
// that ever stops being true, at the cost of a samples slice and string lookups
// that buy nothing at this cadence.
func (m *Metrics) sampleRuntime() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.Goroutines.Store(int64(runtime.NumGoroutine()))
	m.HeapAlloc.Store(int64(ms.HeapAlloc))
	m.HeapObjects.Store(int64(ms.HeapObjects))
	m.HeapSys.Store(int64(ms.HeapSys))
}

// series has a side effect: it samples the runtime gauges before returning, so
// every consumer sees current values. That is here rather than in the two
// callers for the same reason the list itself is here — a caller that forgot to
// sample would export stale numbers, which is the drift this list exists to
// prevent, and harder to notice than a missing metric.
func (m *Metrics) series() []series {
	m.sampleRuntime()
	s := []series{
		{"requests_total", "counter", &m.Requests},
		{"errors_total", "counter", &m.Errors},
		{"retries_total", "counter", &m.Retries},
		{"rate_limited_total", "counter", &m.RateLimited},
		{"usage_mismatch_total", "counter", &m.UsageMismatch},
		{"empty_completion_total", "counter", &m.EmptyCompletion},
		{"empty_completion_recovered_total", "counter", &m.EmptyCompletionRecovered},
		{"empty_completion_failed_total", "counter", &m.EmptyCompletionFailed},
		{"account_failover_total", "counter", &m.AccountFailover},
		{"refused_failover_total", "counter", &m.RefusedFailover},
		{"provider_probe_failed_total", "counter", &m.ProviderProbeFailed},
		{"budget_skip_total", "counter", &m.BudgetSkip},
		{"temperature_dropped_total", "counter", &m.TemperatureDropped},
		{"capture_full_total", "counter", &m.CaptureFull},
		{"idempotency_full_total", "counter", &m.IdemFull},
		{"idempotent_replay_total", "counter", &m.IdempotentReplay},
		{"idempotent_conflict_total", "counter", &m.IdempotentConflict},
		{"idempotent_unknown_total", "counter", &m.IdempotentUnknown},
		{"rejected_total", "counter", &m.Rejected},
		{"telemetry_dropped_total", "counter", &m.Dropped},
		{"telemetry_disk_errors_total", "counter", &m.DiskErrors},
		{"telemetry_export_errors_total", "counter", &m.ExportErrors},
		{"policy_errors_total", "counter", &m.PolicyErrors},
		{"spool_bytes", "gauge", &m.SpoolUsed},
		{"spool_events", "gauge", &m.SpoolCount},
		{"active_requests", "gauge", &m.Active},
		{"goroutines", "gauge", &m.Goroutines},
		{"heap_alloc_bytes", "gauge", &m.HeapAlloc},
		{"heap_objects", "gauge", &m.HeapObjects},
		{"heap_sys_bytes", "gauge", &m.HeapSys},
		{"policy_expires_in_seconds", "gauge", &m.PolicyExpiresIn},
	}
	if m.LogDropped != nil {
		s = append(s, series{"log_dropped_total", "counter", m.LogDropped})
	}
	return s
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, v := range m.series() {
		fmt.Fprintf(w, "# TYPE switchboard_%s %s\nswitchboard_%s %d\n", v.name, v.kind, v.name, v.v.Load())
	}
	fmt.Fprintln(w, "# TYPE switchboard_request_duration_milliseconds histogram")
	for i, b := range latencyBounds {
		fmt.Fprintf(w, "switchboard_request_duration_milliseconds_bucket{le=\"%d\"} %d\n", b, m.LatencyBuckets[i].Load())
	}
	fmt.Fprintf(w, "switchboard_request_duration_milliseconds_bucket{le=\"+Inf\"} %d\nswitchboard_request_duration_milliseconds_sum %d\nswitchboard_request_duration_milliseconds_count %d\n", m.Completed.Load(), m.LatencyMS.Load(), m.Completed.Load())
}

type Event struct {
	ID        string `json:"id"`
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id"`
	SpanID    string `json:"span_id"`
	ParentID  string `json:"parent_id,omitempty"`
	Provider  string `json:"provider"`
	Status    int    `json:"status"`
	Attempts  int    `json:"attempts"`
	Start     int64  `json:"start_ns"`
	End       int64  `json:"end_ns"`
	// PolicyVersion carries a real JSON tag, unlike the two span-only fields
	// below, because it has to reach the control plane: that is where the
	// policies table lives, and the whole value of recording it is the join.
	// Every signed envelope is kept by (tenant, version) forever, so this one
	// number turns a request into a provable routing decision - which policy was
	// live, what its route order was, and therefore why this provider answered.
	// A policy also forbids a repeated provider, so (version, provider)
	// determines the model too, and nothing further needs storing.
	//
	// Recorded before any route is chosen, so a request refused by policy has it
	// as surely as one that reached a provider.
	PolicyVersion int64 `json:"policy_version,omitempty"`
	// Model and Fault were span-only, because the control plane rejected any key
	// its model did not name and one unrecognised field failed the whole event.
	// They now travel in Ext, which exists so that a gateway newer than the
	// control plane it reports to degrades instead of going silent.
	//
	// They are still Go fields rather than map entries: the span builder reads
	// them directly, and a routing decision recorded in a map is a routing
	// decision nothing type-checks.
	Model string `json:"-"`
	Fault string `json:"-"`
	// Usage is what the provider said the request cost. adapter.go already
	// normalises four incompatible usage shapes into these five numbers and,
	// until now, used them only to decide whether a provider's own totals added
	// up -- then dropped them. Nothing downstream could answer what a request
	// cost, which is the first question anyone puts to a gateway.
	//
	// Span-only in the same sense as Model and Fault: it reaches the control
	// plane through Ext, so an older control plane treats it as data rather than
	// failing the whole event.
	Usage tokenUsage `json:"-"`
	// Ext carries everything the core schema does not name. The control plane
	// validates the core strictly and keeps this verbatim, so a field added to
	// the gateway reaches an older control plane as data rather than as a
	// validation failure -- which is the whole point, because the two halves are
	// deployed independently by a customer and there is no ordering to enforce.
	//
	// New optional fields belong here, not at the top level. The core is the
	// part both sides must agree on and is deliberately hard to grow.
	Ext map[string]any `json:"ext,omitempty"`
}

// wire fills Ext from the fields that are not part of the core schema. Done at
// the single point where an event is serialised, so a new field cannot be added
// to Event and forgotten here.
func (e Event) wire() Event {
	ext := map[string]any{}
	for k, v := range e.Ext {
		ext[k] = v
	}
	if e.Model != "" {
		ext["model"] = e.Model
	}
	if e.Fault != "" {
		ext["fault"] = e.Fault
	}
	for _, a := range e.Usage.attrs() {
		ext[a.key] = a.value
	}
	if len(ext) == 0 {
		return e
	}
	e.Ext = ext
	return e
}

func randomID(n int) string {
	b := make([]byte, n)
	if _, e := rand.Read(b); e != nil {
		panic("system randomness unavailable")
	}
	return hex.EncodeToString(b)
}

type Telemetry struct {
	c     Config
	m     *Metrics
	queue chan Event
	otlp  chan Event
	wg    sync.WaitGroup
	http  *http.Client
	dir   string
	// perEvent is set once when the control plane turns out to have no batch
	// route, so the fallback is decided a single time rather than per tick.
	perEvent atomic.Bool
	// start anchors cumulative counters. OTLP requires every point of a
	// cumulative series to carry the same start time, so a consumer can tell a
	// counter reset from a process restart.
	start time.Time
}

func NewTelemetry(c Config, m *Metrics) (*Telemetry, error) {
	dir := filepath.Join(c.DataDir, "spool")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	files, e := os.ReadDir(dir)
	if e != nil {
		return nil, e
	}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".json") {
			i, e := f.Info()
			if e != nil {
				return nil, e
			}
			m.SpoolUsed.Add(i.Size())
			m.SpoolCount.Add(1)
		}
	}
	return &Telemetry{c: c, m: m, queue: make(chan Event, c.QueueSize), otlp: make(chan Event, c.QueueSize), http: client(5 * time.Second), dir: dir, start: time.Now()}, nil
}
func (t *Telemetry) Emit(e Event) {
	if t.c.OTLPURL != "" {
		select {
		case t.otlp <- e:
		default:
			t.m.ExportErrors.Add(1)
		}
	}
	select {
	case t.queue <- e:
	default:
		t.m.Dropped.Add(1)
	}
}

// metricInterval is how often counters are pushed. Slow enough to be
// insignificant against inference latency, fast enough that an alarm on lost
// service is not minutes stale.
const metricInterval = 30 * time.Second

func (t *Telemetry) Start(ctx context.Context) {
	if t.c.OTLPMetricsURL != "" {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			ticker := time.NewTicker(metricInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					t.exportMetrics(ctx)
				}
			}
		}()
	}
	t.wg.Add(3)
	go func() {
		defer t.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-t.otlp:
				// Gathered, not sent one at a time. The spool path was batched
				// for exactly this reason -- one POST per event is round-trip
				// bound, roughly twenty per second across a 50 ms network -- and
				// the OTLP path was left in that shape while being the one the
				// deployment docs point at a hosted backend. A single goroutine
				// doing a blocking POST per span is a hard ceiling on how much
				// tracing a gateway can emit, and the excess is dropped at Emit.
				//
				// OTLP/HTTP already carries many spans in one scopeSpans.spans
				// array, so this is batching within the protocol rather than a
				// change to it.
				t.exportOTLPBatch(ctx, t.gatherSpans(ctx, e))
			}
		}
	}()
	go func() {
		defer t.wg.Done()
		for {
			select {
			case e := <-t.queue:
				t.persist(e)
			case <-ctx.Done():
				for {
					select {
					case e := <-t.queue:
						t.persist(e)
					default:
						return
					}
				}
			}
		}
	}()
	go func() {
		defer t.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				t.deliver(ctx)
			}
		}
	}()
}
func (t *Telemetry) Wait() { t.wg.Wait() }
func (t *Telemetry) persist(e Event) {
	b := jsonBytes(e.wire())
	if t.m.SpoolUsed.Load()+int64(len(b)) > t.c.SpoolBytes || t.m.SpoolCount.Load() >= 10000 {
		t.m.Dropped.Add(1)
		return
	}
	if err := atomicFile(filepath.Join(t.dir, e.ID+".json"), b); err != nil {
		t.m.DiskErrors.Add(1)
		t.m.Dropped.Add(1)
		return
	}
	t.m.SpoolUsed.Add(int64(len(b)))
	t.m.SpoolCount.Add(1)
}

// deliverBatch is how many events go in one request, and deliverBatches how many
// requests one tick may issue. Delivery used to be one POST per event issued
// sequentially, so its real ceiling was round-trip bound: comfortable against a
// control plane in the same task, roughly twenty per second across a network at
// 50 ms. A spool that cannot drain grows to its cap and then drops events, and
// the events are billing and audit records.
//
// deliverBytes bounds a batch by size as well as by count, because the two
// limits governing one request were not derived from each other: the receiver
// rejects a body over 65536 bytes, and 200 fully-populated events measure about
// 64,812 -- roughly 1% of headroom. Adding policy_version put 200 bytes on every
// batch and crossed the cap once a tenant's policy version reached five digits.
//
// The failure that follows has no floor: the control plane answers 413, send()
// returns false, nothing is acknowledged or removed, and the identical over-size
// batch is rebuilt and re-posted every tick until the spool fills and starts
// dropping. Sizing the batch here means the sender cannot construct a request
// the receiver is obliged to refuse; a smaller fixed count would only move the
// cliff to the next field somebody adds.
const (
	deliverBatch   = 200
	deliverBatches = 5
	deliverBytes   = 56 << 10
)

// spooled is one file waiting to be sent, kept with its size so the accounting
// gauges can be corrected exactly when it is removed.
type spooled struct {
	path  string
	id    string
	size  int64
	event json.RawMessage
}

func (t *Telemetry) deliver(ctx context.Context) {
	files, e := os.ReadDir(t.dir)
	if e != nil {
		t.m.DiskErrors.Add(1)
		return
	}
	batch := make([]spooled, 0, deliverBatch)
	batchBytes := 0
	sentBatches := 0
	for _, f := range files {
		if ctx.Err() != nil || sentBatches >= deliverBatches {
			return
		}
		if !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		path := filepath.Join(t.dir, f.Name())
		b, e := os.ReadFile(path)
		if e != nil || len(b) > 8192 {
			t.m.DiskErrors.Add(1)
			continue
		}
		var event Event
		if json.Unmarshal(b, &event) != nil {
			t.m.DiskErrors.Add(1)
			continue
		}
		batch = append(batch, spooled{path: path, id: event.ID, size: int64(len(b)), event: b})
		batchBytes += len(b)
		if len(batch) < deliverBatch && batchBytes < deliverBytes {
			continue
		}
		if !t.send(ctx, batch) {
			return
		}
		batch, batchBytes, sentBatches = batch[:0], 0, sentBatches+1
	}
	if len(batch) > 0 && sentBatches < deliverBatches {
		t.send(ctx, batch)
	}
}

// send delivers one batch and removes exactly what the control plane
// acknowledged. It reports whether the exchange succeeded, so a failing tick
// stops rather than hammering an unreachable control plane.
func (t *Telemetry) send(ctx context.Context, batch []spooled) bool {
	if t.perEvent.Load() {
		return t.sendPerEvent(ctx, batch)
	}
	body := make([]byte, 0, 256*len(batch))
	body = append(body, []byte(`{"events":[`)...)
	for i, s := range batch {
		if i > 0 {
			body = append(body, ',')
		}
		body = append(body, s.event...)
	}
	body = append(body, []byte(`]}`)...)

	res, e := t.post(ctx, "/v1/telemetry/batch", body)
	if e != nil {
		t.m.ExportErrors.Add(1)
		return false
	}
	ack, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	res.Body.Close()
	// An older control plane has no batch route. Fall back for the rest of this
	// process and say so, because "your control plane predates batching" should
	// be visible rather than inferred from throughput.
	if res.StatusCode == 404 || res.StatusCode == 405 {
		if t.perEvent.CompareAndSwap(false, true) {
			slog.Info("control plane has no batch telemetry route; falling back to one request per event",
				"url", trimURL(t.c.ControlURL)+"/v1/telemetry")
		}
		return t.sendPerEvent(ctx, batch)
	}
	// A pointer distinguishes an absent field from an empty list. An empty list is
	// a legitimate answer, meaning every event in the batch was rejected as
	// malformed; an absent one means whatever answered is not the batch route.
	var a struct {
		Accepted *[]string `json:"accepted"`
	}
	if res.StatusCode != 200 {
		t.m.ExportErrors.Add(1)
		return false
	}
	if json.Unmarshal(ack, &a) != nil || a.Accepted == nil {
		// 200 from something that is not the batch route: an older control plane
		// behind a proxy that does not 404, for instance. Falling back is strictly
		// better than looping forever acknowledging nothing, which would grow the
		// spool to its cap and then drop billing records.
		if t.perEvent.CompareAndSwap(false, true) {
			slog.Info("control plane did not answer the batch route as expected; falling back to one request per event",
				"status", res.StatusCode)
		}
		return t.sendPerEvent(ctx, batch)
	}
	ok := make(map[string]bool, len(*a.Accepted))
	for _, id := range *a.Accepted {
		ok[id] = true
	}
	// The control plane processed the whole batch and reported what it took, so
	// anything sent and not acknowledged was refused deterministically and will be
	// refused again. Keeping it would retry it every tick forever and eventually
	// fill the spool, so it is dropped and counted: losing one malformed event is
	// better than losing every event queued behind it.
	for _, sp := range batch {
		if ok[sp.id] {
			t.remove(sp)
			continue
		}
		t.m.Dropped.Add(1)
		t.remove(sp)
	}
	return true
}

// sendPerEvent is the original path, kept for control planes without the batch
// route rather than deleted, so this change is safe to deploy in either order.
func (t *Telemetry) sendPerEvent(ctx context.Context, batch []spooled) bool {
	for _, sp := range batch {
		res, e := t.post(ctx, "/v1/telemetry", sp.event)
		if e != nil {
			t.m.ExportErrors.Add(1)
			return false
		}
		ack, e := io.ReadAll(io.LimitReader(res.Body, 8193))
		res.Body.Close()
		var a struct {
			ID string `json:"id"`
		}
		if e != nil || res.StatusCode != 200 || json.Unmarshal(ack, &a) != nil || a.ID != sp.id {
			t.m.ExportErrors.Add(1)
			// A 4xx that is not 429 is a refusal, not a hiccup: the control plane
			// has looked at this event and will say the same thing forever. The
			// batch path already drops these, on the reasoning that losing one
			// malformed event beats losing every event queued behind it; this
			// path retried them instead, so a single rejected event blocked the
			// spool indefinitely.
			//
			// That is not hypothetical. This is the path a gateway falls back to
			// against any control plane predating the batch route, and every
			// event now carries policy_version, which such a control plane's
			// extra="forbid" model rejects -- so the first event 422s and nothing
			// after it is ever delivered.
			if res.StatusCode >= 400 && res.StatusCode < 500 && res.StatusCode != 429 {
				t.m.Dropped.Add(1)
				t.remove(sp)
				continue
			}
			return false
		}
		t.remove(sp)
	}
	return true
}

func (t *Telemetry) post(ctx context.Context, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", trimURL(t.c.ControlURL)+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv(t.c.ControlTokenEnv))
	req.Header.Set("Content-Type", "application/json")
	return t.http.Do(req)
}

func (t *Telemetry) remove(sp spooled) {
	if os.Remove(sp.path) != nil {
		t.m.DiskErrors.Add(1)
		return
	}
	t.m.SpoolUsed.Add(-sp.size)
	t.m.SpoolCount.Add(-1)
}

// exportMetrics pushes every counter and gauge over OTLP. It exists because
// nothing collects /metrics in a shipped deployment: the endpoint is bound to
// loopback with the rest of the gateway, and the collector that would scrape it
// is an opt-in sidecar absent from both CloudFormation templates and the sample
// task definition. Without this, every counter here is unreadable in production.
//
// Two histograms go out beside the counters, both dimensioned per route:
// gen_ai.server.request.duration and gen_ai.client.token.usage. The comment
// here used to say histograms were deliberately not exported, which stopped
// being true the moment one was added twenty lines below it.
func (t *Telemetry) exportMetrics(ctx context.Context) {
	now := strconv.FormatInt(time.Now().UnixNano(), 10)
	start := strconv.FormatInt(t.start.UnixNano(), 10)
	metrics := make([]any, 0, 20)
	for _, s := range t.m.series() {
		point := map[string]any{"asInt": strconv.FormatInt(s.v.Load(), 10), "timeUnixNano": now}
		m := map[string]any{"name": "switchboard." + s.name, "unit": "1"}
		if s.kind == "counter" {
			point["startTimeUnixNano"] = start
			// 2 is CUMULATIVE: the value is a running total since start, not a
			// delta, which is what an atomic counter actually is.
			m["sum"] = map[string]any{"aggregationTemporality": 2, "isMonotonic": true, "dataPoints": []any{point}}
		} else {
			m["gauge"] = map[string]any{"dataPoints": []any{point}}
		}
		metrics = append(metrics, m)
	}
	metrics = append(metrics, t.routeHistograms(now, start)...)
	body := map[string]any{"resourceMetrics": []any{map[string]any{
		"resource":     map[string]any{"attributes": []any{map[string]any{"key": "service.name", "value": map[string]any{"stringValue": "switchboard-gateway"}}}},
		"scopeMetrics": []any{map[string]any{"scope": map[string]any{"name": "switchboard", "version": "1.0.0"}, "metrics": metrics}},
	}}}
	req, err := http.NewRequestWithContext(ctx, "POST", t.c.OTLPMetricsURL, bytes.NewReader(jsonBytes(body)))
	if err != nil {
		t.m.ExportErrors.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	t.otlpHeaders(req)
	res, err := t.http.Do(req)
	if err != nil {
		t.m.ExportErrors.Add(1)
		return
	}
	b, _ := io.ReadAll(io.LimitReader(res.Body, 8192))
	res.Body.Close()
	if res.StatusCode != 200 || bytes.Contains(b, []byte("rejectedDataPoints")) {
		t.m.ExportErrors.Add(1)
	}
}

// routeHistograms encodes the two per-route histograms the conventions define.
//
// The one thing here that is easy to get silently wrong: Prometheus buckets are
// cumulative and OTLP bucketCounts are not. Our buckets[i] holds every request
// at or below latencyBounds[i], including all earlier buckets, while OTLP wants
// the count falling within each bucket plus one overflow bucket. Emitting the
// cumulative values directly would produce a plausible-looking histogram that is
// wrong everywhere except the first bucket.
//
// len(bucketCounts) must be exactly len(explicitBounds)+1 or consumers reject or
// misread the point.
//
// Durations are stored in milliseconds and the convention specifies seconds, so
// bounds and sums are divided here and only here. A factor of a thousand is the
// whole risk in this function and it is invisible to anything checking the name.
func (t *Telemetry) routeHistograms(now, start string) []any {
	routes := t.m.routes.snapshot()
	if len(routes) == 0 {
		return nil
	}
	bounds := make([]any, 0, len(latencyBounds))
	for _, b := range latencyBounds {
		bounds = append(bounds, float64(b)/1000)
	}
	duration := make([]any, 0, len(routes))
	tokens := make([]any, 0, 2*len(routes))
	for k, r := range routes {
		// Read each counter once. They are individually atomic but not a
		// consistent snapshot, so differencing re-read values could produce
		// nonsense.
		cumulative := make([]int64, len(latencyBounds))
		for i := range latencyBounds {
			cumulative[i] = r.buckets[i].Load()
		}
		count := r.count.Load()
		counts := make([]string, 0, len(latencyBounds)+1)
		prev := int64(0)
		for _, c := range cumulative {
			// Clamped: a request completing between two of the loads above can
			// leave a difference momentarily negative, which is not a real value.
			counts = append(counts, strconv.FormatInt(max(c-prev, 0), 10))
			prev = c
		}
		counts = append(counts, strconv.FormatInt(max(count-prev, 0), 10)) // +Inf
		duration = append(duration, map[string]any{
			"startTimeUnixNano": start,
			"timeUnixNano":      now,
			"count":             strconv.FormatInt(count, 10),
			"sum":               float64(r.sumMS.Load()) / 1000,
			"bucketCounts":      counts,
			"explicitBounds":    bounds,
			"attributes":        k.attrs(true),
		})
		// One point per token type, which is how gen_ai.token.type dimensions
		// this metric. Zero is not reported: a route that recorded no tokens has
		// not measured zero of them.
		for _, tt := range []struct {
			kind  string
			total int64
		}{{"input", r.inputTokens.Load()}, {"output", r.outputTokens.Load()}} {
			if tt.total == 0 {
				continue
			}
			attrs := append(k.attrs(false),
				map[string]any{"key": "gen_ai.token.type", "value": map[string]any{"stringValue": tt.kind}})
			tokens = append(tokens, map[string]any{
				"startTimeUnixNano": start,
				"timeUnixNano":      now,
				"count":             strconv.FormatInt(count, 10),
				"sum":               float64(tt.total),
				"bucketCounts":      []string{"0", strconv.FormatInt(count, 10)},
				"explicitBounds":    []any{0},
				"attributes":        attrs,
			})
		}
	}
	out := []any{map[string]any{
		// server, not client: this is the duration the caller waited. A request
		// that failed over is attributed to the provider that answered while its
		// duration includes the ones that did not -- correct for a server metric,
		// and not what a per-provider latency reading naively suggests.
		"name": "gen_ai.server.request.duration",
		"unit": "s",
		"histogram": map[string]any{
			"aggregationTemporality": 2,
			"dataPoints":             duration,
		},
	}}
	if len(tokens) > 0 {
		out = append(out, map[string]any{
			"name": "gen_ai.client.token.usage",
			"unit": "{token}",
			"histogram": map[string]any{
				"aggregationTemporality": 2,
				"dataPoints":             tokens,
			},
		})
	}
	return out
}

// attrs renders the key in the conventions' vocabulary. operation and provider
// are Required on both metrics and come from the same table the spans use, so a
// provider cannot be named one way on a span and another on a metric.
//
// model is Conditionally Required "if available" -- it is omitted exactly when
// it is not, which is a route folded past the table cap. withError adds
// error.type, which the duration metric requires on a failure and the token
// metric does not define.
func (k routeKey) attrs(withError bool) []any {
	sc := semconvOf(k.provider)
	out := []any{
		map[string]any{"key": "gen_ai.operation.name", "value": map[string]any{"stringValue": sc.operation}},
		map[string]any{"key": "gen_ai.provider.name", "value": map[string]any{"stringValue": sc.provider}},
	}
	if k.model != "" {
		out = append(out, map[string]any{"key": "gen_ai.request.model", "value": map[string]any{"stringValue": k.model}})
	}
	if withError && k.errorType != "" {
		out = append(out, map[string]any{"key": "error.type", "value": map[string]any{"stringValue": k.errorType}})
	}
	return out
}

// otlpHeaders applies the configured headers, reading each value from the
// environment at request time so no credential is held in memory longer than the
// request or written into the configuration file. Set after the exporter's own
// headers, and validated at startup so it cannot replace them.
func (t *Telemetry) otlpHeaders(req *http.Request) {
	for name, env := range t.c.OTLPHeaders {
		if v := os.Getenv(env); v != "" {
			req.Header.Set(name, v)
		}
	}
}

// otlpBatch and otlpFlush bound a span batch the way deliverBatch and
// deliverBytes bound the spool: by count and by time, so a quiet gateway does
// not hold a span waiting for a batch that will not fill.
const (
	otlpBatch = 64
	otlpFlush = 200 * time.Millisecond
)

// gatherSpans collects whatever is already queued behind the first event,
// without waiting for more than otlpFlush. Under load this fills immediately;
// idle, it returns the one span it was given.
func (t *Telemetry) gatherSpans(ctx context.Context, first Event) []Event {
	batch := make([]Event, 0, otlpBatch)
	batch = append(batch, first)
	timer := time.NewTimer(otlpFlush)
	defer timer.Stop()
	for len(batch) < otlpBatch {
		select {
		case <-ctx.Done():
			return batch
		case e := <-t.otlp:
			batch = append(batch, e)
		case <-timer.C:
			return batch
		}
	}
	return batch
}

// exportOTLPBatch sends many spans as one request.
func (t *Telemetry) exportOTLPBatch(ctx context.Context, events []Event) {
	if len(events) == 0 {
		return
	}
	spans := make([]any, 0, len(events))
	for _, e := range events {
		spans = append(spans, t.spanOf(e))
	}
	t.postSpans(ctx, spans)
}

// OTLP/HTTP JSON encoding follows the OpenTelemetry protobuf JSON mapping.
//
// spanOf builds one span; postSpans sends any number of them. Split so the
// batching above shares the encoding rather than duplicating it -- the span
// attributes here are the product of several corrections and must not exist in
// two places.
// semconv describes one provider in the GenAI semantic conventions' vocabulary,
// which is not ours and cannot be made ours.
//
// Our four identifiers are a wire contract: controlplane/app.py validates them
// as a Literal, every signed policy carries them, and policy.go:136 enforces
// them. Renaming them to match the convention would break stored policies and
// every gateway older than the control plane. So the translation lives here, at
// the single boundary where the convention applies -- which is exactly the job
// an external normalizer would be doing, done by the emitter that already knows
// the answer.
//
// Two of the four already conform. The other two did not, silently: nothing
// rejects an off-enum value, so bedrock and gemini traffic simply sat outside
// every GenAI-aware backend's provider grouping, and gen_ai.provider.name is
// the documented discriminator the rest of the span is read through.
//
// operation is per provider rather than a constant because the convention says
// a well-known value MUST be used where one applies. Gemini is reached at
// generativelanguage.googleapis.com/...:generateContent, which is what
// generate_content names and what scopes it to gcp.gemini rather than
// gcp.vertex_ai (aiplatform.googleapis.com). The other three are chat APIs,
// Bedrock included -- Converse is a chat operation.
// tokenUsage is what a request cost, in the five figures the conventions name.
// CacheRead and CacheWrite are parts of Input, not additions to it.
type tokenUsage struct {
	Input, Output, Reasoning, CacheRead, CacheWrite int
}

type usageAttr struct {
	key   string
	value int
}

// attrs names the usage in the conventions' vocabulary, in a fixed order so an
// exported attribute list is stable rather than map-ordered.
//
// Input and Output are reported together whenever either is set, so any request
// that reached a provider carries both. An output of zero is a measurement, not
// a gap: a reasoning model that spends its entire budget before writing a word
// reports exactly that, and it is the case this gateway exists to notice.
//
// The other three are reported only when non-zero. They are Recommended "when
// applicable", and a provider with no cache and no reasoning has not measured
// zero of them -- it has not measured them. Emitting zeros would put a number
// on three-quarters of the fleet that means "unknown".
func (u tokenUsage) attrs() []usageAttr {
	if u.Input == 0 && u.Output == 0 {
		return nil
	}
	out := []usageAttr{
		{"gen_ai.usage.input_tokens", u.Input},
		{"gen_ai.usage.output_tokens", u.Output},
	}
	for _, a := range []usageAttr{
		{"gen_ai.usage.reasoning.output_tokens", u.Reasoning},
		{"gen_ai.usage.cache_read.input_tokens", u.CacheRead},
		{"gen_ai.usage.cache_write.input_tokens", u.CacheWrite},
	} {
		if a.value > 0 {
			out = append(out, a)
		}
	}
	return out
}

type semconv struct{ provider, operation string }

var semconvProviders = map[string]semconv{
	"openai":    {"openai", "chat"},
	"anthropic": {"anthropic", "chat"},
	"gemini":    {"gcp.gemini", "generate_content"},
	"bedrock":   {"aws.bedrock", "chat"},
}

// semconvOf falls back to the identifier unchanged with the default operation.
// A fifth provider added before this table is updated then emits a value that
// is merely unrecognised, which is what it was before; the alternative is an
// empty required attribute, which is worse.
func semconvOf(provider string) semconv {
	if s, ok := semconvProviders[provider]; ok {
		return s
	}
	return semconv{provider, "chat"}
}

func (t *Telemetry) spanOf(e Event) map[string]any {
	sc := semconvOf(e.Provider)
	attrs := []any{
		map[string]any{"key": "gen_ai.provider.name", "value": map[string]any{"stringValue": sc.provider}},
		// Required by the convention on both the span and the token-usage metric,
		// and previously absent -- which is what made these spans non-conformant
		// rather than merely sparse, since it is the primary grouping key.
		map[string]any{"key": "gen_ai.operation.name", "value": map[string]any{"stringValue": sc.operation}},
		map[string]any{"key": "http.response.status_code", "value": map[string]any{"intValue": strconv.Itoa(e.Status)}},
		// What the routing loop decided, which the span used to drop on the floor.
		// The metrics already count how often failover happens; without these,
		// nothing answers what happened to one particular request, which is the
		// only question a trace exists to answer. All three are read from values
		// the request already had, so this costs nothing to produce.
		//
		// gen_ai.request.model is the model sent upstream, which is what semconv
		// means by "requested" — of the provider, not of the gateway. attempts and
		// fault stay under switchboard.* because they describe routing, and no
		// OpenTelemetry convention covers a router yet; an experimental namespace
		// is what OpenTelemetry asks for while that is true.
		map[string]any{"key": "switchboard.attempts", "value": map[string]any{"intValue": strconv.Itoa(e.Attempts)}},
	}
	if e.PolicyVersion > 0 {
		// On the span as well as the event, so a trace answers "which policy sent
		// it here" without a database round trip.
		attrs = append(attrs, map[string]any{"key": "switchboard.policy_version",
			"value": map[string]any{"intValue": strconv.FormatInt(e.PolicyVersion, 10)}})
	}
	if e.Model != "" {
		attrs = append(attrs, map[string]any{"key": "gen_ai.request.model", "value": map[string]any{"stringValue": e.Model}})
	}
	if e.Fault != "" {
		attrs = append(attrs, map[string]any{"key": "switchboard.fault", "value": map[string]any{"stringValue": e.Fault}})
	}
	// What the request cost, from the same table wire() reads, so the span and
	// the control plane cannot disagree about the names.
	for _, a := range e.Usage.attrs() {
		attrs = append(attrs, map[string]any{"key": a.key, "value": map[string]any{"intValue": strconv.Itoa(a.value)}})
	}
	// "{gen_ai.operation.name} {gen_ai.request.model}", which the convention says
	// a span name SHOULD be. The old name was switchboard.inference: correct
	// about what this is and unreadable to anything that groups GenAI spans by
	// the convention's shape.
	//
	// Model is empty on a request refused before any route was chosen, and
	// "chat " with a trailing space is not a name. Fall back to the operation
	// alone, which is still the convention's first half rather than a third
	// vocabulary.
	name := sc.operation
	if e.Model != "" {
		name += " " + e.Model
	}
	span := map[string]any{"traceId": e.TraceID, "spanId": e.SpanID, "name": name, "kind": 2, "startTimeUnixNano": strconv.FormatInt(e.Start, 10), "endTimeUnixNano": strconv.FormatInt(e.End, 10), "attributes": attrs}
	if e.ParentID != "" {
		span["parentSpanId"] = e.ParentID
	}
	if e.Status >= 400 {
		span["status"] = map[string]any{"code": 2}
		// The span status above is correct and not enough on its own: Honeycomb's
		// error-rate detection reads error.type, error.message, exception.type and
		// exception.message, and ignores the status. A service whose spans carry
		// only a status is reported as having no recognised error attributes and
		// is refused monitoring, which is a silent gap of exactly the kind this
		// telemetry exists to close.
		//
		// The value is the status code, per OTel's HTTP convention for an error
		// with no exception class behind it. It is also the honest taxonomy here:
		// every fail() in server.go picks a distinct status for a distinct cause,
		// so a second vocabulary would only restate the first at higher
		// cardinality on the field a rate is aggregated over.
		//
		// error.message is deliberately absent. Some fail() messages are derived
		// from a provider response or from decoding the request body, and
		// docs/SECURITY.md promises neither appears in product telemetry. One
		// recognised attribute is enough to be monitored; it is not worth a
		// written guarantee.
		span["attributes"] = append(span["attributes"].([]any),
			map[string]any{"key": "error.type", "value": map[string]any{"stringValue": strconv.Itoa(e.Status)}})
	}
	return span
}

// exportOTLP sends a single span. Kept for the tests that assert on one span's
// shape; the serving path batches through exportOTLPBatch.
func (t *Telemetry) exportOTLP(ctx context.Context, e Event) {
	t.postSpans(ctx, []any{t.spanOf(e)})
}

func (t *Telemetry) postSpans(ctx context.Context, spans []any) {
	body := map[string]any{"resourceSpans": []any{map[string]any{"resource": map[string]any{"attributes": []any{map[string]any{"key": "service.name", "value": map[string]any{"stringValue": "switchboard-gateway"}}}}, "scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "switchboard", "version": "1.0.0"}, "spans": spans}}}}}
	req, _ := http.NewRequestWithContext(ctx, "POST", t.c.OTLPURL, bytes.NewReader(jsonBytes(body)))
	req.Header.Set("Content-Type", "application/json")
	t.otlpHeaders(req)
	res, err := t.http.Do(req)
	if err != nil {
		t.m.ExportErrors.Add(1)
		return
	}
	b, _ := io.ReadAll(io.LimitReader(res.Body, 8192))
	res.Body.Close()
	if res.StatusCode != 200 || bytes.Contains(b, []byte("rejectedSpans")) {
		t.m.ExportErrors.Add(1)
	}
}
