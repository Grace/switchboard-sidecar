package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDurableReplayAndAck(t *testing.T) {
	var ok atomic.Bool
	cp := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ok.Load() {
			w.WriteHeader(503)
			return
		}
		var e Event
		json.NewDecoder(r.Body).Decode(&e)
		w.Write(jsonBytes(map[string]string{"id": e.ID}))
	}))
	defer cp.Close()
	c := Config{DataDir: t.TempDir(), ControlURL: cp.URL, QueueSize: 1, SpoolBytes: 1 << 20}
	m := &Metrics{}
	tel, e := NewTelemetry(c, m)
	if e != nil {
		t.Fatal(e)
	}
	useTestTransport(tel.http)
	tel.persist(Event{ID: randomID(16)})
	tel.deliver(context.Background())
	if m.SpoolCount.Load() != 1 {
		t.Fatal("unacknowledged event deleted")
	}
	m2 := &Metrics{}
	restarted, e := NewTelemetry(c, m2)
	if e != nil {
		t.Fatal(e)
	}
	if m2.SpoolCount.Load() != 1 {
		t.Fatal("spool lost on restart")
	}
	ok.Store(true)
	useTestTransport(restarted.http)
	restarted.deliver(context.Background())
	if m2.SpoolCount.Load() != 0 {
		t.Fatal("ack did not remove event")
	}
	files, _ := os.ReadDir(restarted.dir)
	if len(files) != 0 {
		t.Fatal("spool not empty")
	}
}
func TestTelemetryNeverWaitsForDiskOrNetwork(t *testing.T) {
	c := Config{DataDir: t.TempDir(), QueueSize: 1, SpoolBytes: 1}
	m := &Metrics{}
	tel, _ := NewTelemetry(c, m)
	start := time.Now()
	for i := 0; i < 10000; i++ {
		tel.Emit(Event{})
	}
	if time.Since(start) > time.Second || m.Dropped.Load() != 9999 {
		t.Fatal("queue blocked")
	}
	tel.persist(Event{ID: randomID(16)})
	if m.SpoolCount.Load() != 0 {
		t.Fatal("disk cap ignored")
	}
}
func TestOTLPIndependentOfControlPlane(t *testing.T) {
	got := make(chan bool, 1)
	collector := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["resourceSpans"] != nil {
			got <- true
		}
		w.Write([]byte(`{}`))
	}))
	defer collector.Close()
	c := Config{DataDir: t.TempDir(), ControlURL: "http://127.0.0.1:1", OTLPURL: collector.URL, QueueSize: 2, SpoolBytes: 1 << 20}
	tel, _ := NewTelemetry(c, &Metrics{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	useTestTransport(tel.http)
	tel.Start(ctx)
	tel.Emit(Event{ID: randomID(16), TraceID: randomID(16), SpanID: randomID(8), Start: 1, End: 2})
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("OTLP blocked on control plane")
	}
	cancel()
	tel.Wait()
}

// A counter that is declared but left out of the export list is invisible, which
// is the same as not having it. These three exist to make provider faults
// observable, so silently not exporting them would defeat the point.
func TestProviderFaultCountersAreExported(t *testing.T) {
	m := &Metrics{}
	m.EmptyCompletion.Add(3)
	m.AccountFailover.Add(2)
	m.ProviderProbeFailed.Add(1)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{
		"switchboard_empty_completion_total 3",
		"switchboard_account_failover_total 2",
		"switchboard_provider_probe_failed_total 1",
	} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("metrics output is missing %q", want)
		}
	}
}

// Nothing collects /metrics in a shipped deployment: the endpoint is loopback
// only and the scraping collector is an opt-in sidecar absent from both
// templates. These assert the push path that makes the counters readable.
func TestOTLPMetricsExport(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := &Metrics{}
	m.EmptyCompletionFailed.Add(7)
	m.Active.Add(3)
	tel := &Telemetry{c: Config{OTLPMetricsURL: srv.URL}, m: m, http: srv.Client(), start: time.Now()}
	tel.exportMetrics(context.Background())

	var body struct {
		ResourceMetrics []struct {
			ScopeMetrics []struct {
				Metrics []struct {
					Name string `json:"name"`
					Sum  *struct {
						AggregationTemporality int  `json:"aggregationTemporality"`
						IsMonotonic            bool `json:"isMonotonic"`
						DataPoints             []struct {
							AsInt             string `json:"asInt"`
							StartTimeUnixNano string `json:"startTimeUnixNano"`
						} `json:"dataPoints"`
					} `json:"sum"`
					Gauge *struct {
						DataPoints []struct {
							AsInt             string `json:"asInt"`
							StartTimeUnixNano string `json:"startTimeUnixNano"`
						} `json:"dataPoints"`
					} `json:"gauge"`
				} `json:"metrics"`
			} `json:"scopeMetrics"`
		} `json:"resourceMetrics"`
	}
	if err := json.Unmarshal(got, &body); err != nil {
		t.Fatalf("exported body is not valid OTLP JSON: %v\n%s", err, got)
	}
	if len(body.ResourceMetrics) != 1 || len(body.ResourceMetrics[0].ScopeMetrics) != 1 {
		t.Fatalf("unexpected envelope: %s", got)
	}
	seen := map[string]bool{}
	for _, mt := range body.ResourceMetrics[0].ScopeMetrics[0].Metrics {
		seen[mt.Name] = true
		switch mt.Name {
		case "switchboard.empty_completion_failed_total":
			if mt.Sum == nil {
				t.Fatal("a counter was not exported as a sum")
			}
			// Cumulative and monotonic: the value is a running total, and a
			// consumer must be able to tell a reset from a restart.
			if mt.Sum.AggregationTemporality != 2 || !mt.Sum.IsMonotonic {
				t.Errorf("counter temporality=%d monotonic=%v", mt.Sum.AggregationTemporality, mt.Sum.IsMonotonic)
			}
			if mt.Sum.DataPoints[0].AsInt != "7" {
				t.Errorf("value = %q, want 7", mt.Sum.DataPoints[0].AsInt)
			}
			if mt.Sum.DataPoints[0].StartTimeUnixNano == "" {
				t.Error("cumulative point carries no start time")
			}
		case "switchboard.active_requests":
			if mt.Gauge == nil {
				t.Fatal("a gauge was exported as a sum")
			}
			if mt.Gauge.DataPoints[0].AsInt != "3" {
				t.Errorf("gauge = %q, want 3", mt.Gauge.DataPoints[0].AsInt)
			}
		}
	}
	// The whole point of the shared series() list: a counter added in one place
	// and forgotten in the other is invisible exactly where someone looks.
	for _, want := range []string{
		"switchboard.requests_total",
		"switchboard.empty_completion_recovered_total",
		"switchboard.empty_completion_failed_total",
		"switchboard.account_failover_total",
		"switchboard.active_requests",
	} {
		if !seen[want] {
			t.Errorf("metric %q was not exported", want)
		}
	}
}

func TestOTLPMetricsCountsExportFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer srv.Close()
	m := &Metrics{}
	tel := &Telemetry{c: Config{OTLPMetricsURL: srv.URL}, m: m, http: srv.Client(), start: time.Now()}
	tel.exportMetrics(context.Background())
	if m.ExportErrors.Load() != 1 {
		t.Errorf("ExportErrors = %d, want 1", m.ExportErrors.Load())
	}
}

// An unset endpoint must send nothing at all, so a deployment with no collector
// is not making a request every interval to somewhere it was never told about.
func TestOTLPMetricsDisabledWhenURLEmpty(t *testing.T) {
	var called atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called.Add(1) }))
	defer srv.Close()
	tel := &Telemetry{c: Config{}, m: &Metrics{}, queue: make(chan Event, 1), otlp: make(chan Event, 1),
		http: srv.Client(), dir: t.TempDir(), start: time.Now()}
	ctx, cancel := context.WithCancel(context.Background())
	tel.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
	tel.wg.Wait()
	if called.Load() != 0 {
		t.Errorf("exported %d times with no endpoint configured", called.Load())
	}
}

// Prometheus buckets are cumulative; OTLP bucketCounts are not. Emitting the
// cumulative values directly would produce a histogram that looks plausible and
// is wrong in every bucket but the first, which is worse than not exporting one.
func TestOTLPHistogramBucketsAreDifferenced(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := &Metrics{}
	// latencyBounds is [5 25 100 500 1000 5000 15000 60000 90000].
	// 10ms and 50ms straddle the 25ms bound, 300ms falls under 500, and
	// 200000ms is past every bound.
	for _, ms := range []int64{10, 50, 300, 200000} {
		m.ObserveLatency(ms)
	}
	tel := &Telemetry{c: Config{OTLPMetricsURL: srv.URL}, m: m, http: srv.Client(), start: time.Now()}
	tel.exportMetrics(context.Background())

	var body struct {
		ResourceMetrics []struct {
			ScopeMetrics []struct {
				Metrics []struct {
					Name      string `json:"name"`
					Histogram *struct {
						AggregationTemporality int `json:"aggregationTemporality"`
						DataPoints             []struct {
							Count          string   `json:"count"`
							Sum            float64  `json:"sum"`
							BucketCounts   []string `json:"bucketCounts"`
							ExplicitBounds []int64  `json:"explicitBounds"`
						} `json:"dataPoints"`
					} `json:"histogram"`
				} `json:"metrics"`
			} `json:"scopeMetrics"`
		} `json:"resourceMetrics"`
	}
	if err := json.Unmarshal(got, &body); err != nil {
		t.Fatalf("not valid OTLP JSON: %v", err)
	}
	var h *struct {
		Count          string   `json:"count"`
		Sum            float64  `json:"sum"`
		BucketCounts   []string `json:"bucketCounts"`
		ExplicitBounds []int64  `json:"explicitBounds"`
	}
	var temporality int
	for _, mt := range body.ResourceMetrics[0].ScopeMetrics[0].Metrics {
		if mt.Name == "switchboard.request_duration_milliseconds" {
			if mt.Histogram == nil || len(mt.Histogram.DataPoints) != 1 {
				t.Fatal("histogram missing or has no data point")
			}
			temporality = mt.Histogram.AggregationTemporality
			h = &mt.Histogram.DataPoints[0]
		}
	}
	if h == nil {
		t.Fatalf("latency histogram was not exported: %s", got)
	}
	if temporality != 2 {
		t.Errorf("aggregationTemporality = %d, want 2 (cumulative)", temporality)
	}
	// The invariant a consumer relies on.
	if len(h.BucketCounts) != len(h.ExplicitBounds)+1 {
		t.Fatalf("bucketCounts=%d bounds=%d; must differ by exactly one",
			len(h.BucketCounts), len(h.ExplicitBounds))
	}
	if len(h.ExplicitBounds) != len(latencyBounds) {
		t.Errorf("bounds = %v, want %v", h.ExplicitBounds, latencyBounds)
	}
	// The arithmetic check that catches a cumulative-vs-per-bucket mistake even
	// when every individual value looks reasonable.
	var total int64
	for _, c := range h.BucketCounts {
		n, err := strconv.ParseInt(c, 10, 64)
		if err != nil {
			t.Fatalf("bucket count %q is not an integer", c)
		}
		if n < 0 {
			t.Errorf("negative bucket count %d", n)
		}
		total += n
	}
	if h.Count != "4" || total != 4 {
		t.Errorf("count=%q, buckets sum to %d; want both 4", h.Count, total)
	}
	// And by value, not only by sum. This is also the assertion about the floor:
	// 10ms and 50ms are both under the old 100ms first bound, and they must land
	// in *different* buckets. A histogram whose first bucket swallows everything
	// fast still passes every check above, and is the state that made Honeycomb
	// report a negative P50 — the first bucket has no lower edge, so a percentile
	// inside it is extrapolation below zero.
	if h.BucketCounts[1] != "1" || h.BucketCounts[2] != "1" {
		t.Errorf("10ms and 50ms landed in buckets %q/%q, want one each in [1] and [2]; "+
			"anything below latencyBounds[0] is unmeasurable",
			h.BucketCounts[1], h.BucketCounts[2])
	}
	if h.BucketCounts[0] != "0" {
		t.Errorf("bucket at or below %dms = %q, want 0", latencyBounds[0], h.BucketCounts[0])
	}
	if h.BucketCounts[3] != "1" {
		t.Errorf("bucket for 300ms = %q, want 1", h.BucketCounts[3])
	}
	// 200000ms exceeds every bound and belongs in the overflow bucket.
	if h.BucketCounts[len(h.BucketCounts)-1] != "1" {
		t.Errorf("overflow bucket = %q, want 1 (200000ms)", h.BucketCounts[len(h.BucketCounts)-1])
	}
	if h.Sum != float64(m.LatencyMS.Load()) {
		t.Errorf("sum = %v, want %d", h.Sum, m.LatencyMS.Load())
	}
}

// The runtime gauges exist to attribute the linear memory growth recorded in
// docs/VALIDATION.md. They are sampled rather than counted, so the thing worth
// asserting is that a consumer sees live values and that both export paths
// agree on their type.
func TestRuntimeGaugesAreExported(t *testing.T) {
	m := &Metrics{}
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	out := w.Body.String()

	for _, name := range []string{"goroutines", "heap_alloc_bytes", "heap_objects", "heap_sys_bytes"} {
		if !strings.Contains(out, "# TYPE switchboard_"+name+" gauge") {
			t.Errorf("%s is not declared as a gauge", name)
		}
	}
	// A zero here would mean the sample never ran: a live process always has at
	// least the goroutine running this test.
	if m.Goroutines.Load() < 1 {
		t.Errorf("goroutines = %d, want at least 1", m.Goroutines.Load())
	}
	if m.HeapAlloc.Load() < 1 || m.HeapSys.Load() < 1 || m.HeapObjects.Load() < 1 {
		t.Errorf("heap gauges not sampled: alloc=%d objects=%d sys=%d",
			m.HeapAlloc.Load(), m.HeapObjects.Load(), m.HeapSys.Load())
	}
}

// series() samples on every call rather than once, so a scrape reports the
// process as it is now and not as it was when the struct was built.
func TestRuntimeGaugesResampleOnEveryRead(t *testing.T) {
	m := &Metrics{}
	const sentinel = -1
	for i := 0; i < 2; i++ {
		m.Goroutines.Store(sentinel)
		m.HeapAlloc.Store(sentinel)
		m.series()
		if m.Goroutines.Load() == sentinel || m.HeapAlloc.Load() == sentinel {
			t.Fatalf("read %d did not resample: goroutines=%d heap_alloc=%d",
				i+1, m.Goroutines.Load(), m.HeapAlloc.Load())
		}
	}
}

// The series list exists so the Prometheus endpoint and the OTLP exporter
// cannot drift. A gauge exported as a cumulative sum would be read as a running
// total and misinterpreted, so assert the shape rather than only the name.
func TestRuntimeGaugesExportAsGaugesOverOTLP(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	tel := &Telemetry{c: Config{OTLPMetricsURL: srv.URL}, m: &Metrics{}, http: srv.Client(), start: time.Now()}
	tel.exportMetrics(context.Background())

	var body struct {
		ResourceMetrics []struct {
			ScopeMetrics []struct {
				Metrics []struct {
					Name  string          `json:"name"`
					Sum   json.RawMessage `json:"sum"`
					Gauge *struct {
						DataPoints []struct {
							AsInt string `json:"asInt"`
						} `json:"dataPoints"`
					} `json:"gauge"`
				} `json:"metrics"`
			} `json:"scopeMetrics"`
		} `json:"resourceMetrics"`
	}
	if err := json.Unmarshal(got, &body); err != nil {
		t.Fatalf("exported body is not valid OTLP JSON: %v\n%s", err, got)
	}
	want := map[string]bool{
		"switchboard.goroutines": false, "switchboard.heap_alloc_bytes": false,
		"switchboard.heap_objects": false, "switchboard.heap_sys_bytes": false,
	}
	for _, mt := range body.ResourceMetrics[0].ScopeMetrics[0].Metrics {
		if _, ok := want[mt.Name]; !ok {
			continue
		}
		if mt.Sum != nil {
			t.Errorf("%s exported as a sum; it is a gauge", mt.Name)
		}
		if mt.Gauge == nil || len(mt.Gauge.DataPoints) != 1 {
			t.Errorf("%s has no gauge data point", mt.Name)
			continue
		}
		if v, err := strconv.ParseInt(mt.Gauge.DataPoints[0].AsInt, 10, 64); err != nil || v < 1 {
			t.Errorf("%s = %q, want a positive sampled value", mt.Name, mt.Gauge.DataPoints[0].AsInt)
		}
		want[mt.Name] = true
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("%s missing from the OTLP payload", name)
		}
	}
}

// spoolN persists n events and returns their ids.
func spoolN(t *testing.T, tel *Telemetry, n int) []string {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = randomID(16)
		tel.persist(Event{ID: ids[i], Start: 1, End: 2})
	}
	return ids
}

// Delivery used to be one POST per event issued sequentially, which made its real
// ceiling round-trip bound rather than the documented hundred per tick. A spool
// that cannot drain drops billing and audit records.
func TestBatchDeliverySendsOneRequestForManyEvents(t *testing.T) {
	var requests atomic.Int64
	var got atomic.Int64
	cp := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var body struct {
			Events []Event `json:"events"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		got.Add(int64(len(body.Events)))
		ids := make([]string, 0, len(body.Events))
		for _, e := range body.Events {
			ids = append(ids, e.ID)
		}
		w.Write(jsonBytes(map[string]any{"accepted": ids}))
	}))
	defer cp.Close()
	c := Config{DataDir: t.TempDir(), ControlURL: cp.URL, QueueSize: 1, SpoolBytes: 1 << 20}
	m := &Metrics{}
	tel, err := NewTelemetry(c, m)
	if err != nil {
		t.Fatal(err)
	}
	useTestTransport(tel.http)
	spoolN(t, tel, 50)

	tel.deliver(context.Background())
	if requests.Load() != 1 {
		t.Fatalf("%d requests for 50 events; batching did not happen", requests.Load())
	}
	if got.Load() != 50 {
		t.Fatalf("control plane saw %d events, want 50", got.Load())
	}
	if m.SpoolCount.Load() != 0 {
		t.Fatalf("SpoolCount = %d after a full ack", m.SpoolCount.Load())
	}
}

// The control plane processes the whole batch and reports what it took, so an
// event it did not acknowledge was refused deterministically and would be
// refused again. Keeping it would retry it every tick forever and eventually fill
// the spool, so it is dropped and counted. Losing one malformed event is better
// than losing every event queued behind it.
func TestUnacknowledgedEventsAreDroppedNotRetriedForever(t *testing.T) {
	var requests atomic.Int64
	cp := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var body struct {
			Events []Event `json:"events"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		// Accept only the first, as though the rest failed validation.
		w.Write(jsonBytes(map[string]any{"accepted": []string{body.Events[0].ID}}))
	}))
	defer cp.Close()
	c := Config{DataDir: t.TempDir(), ControlURL: cp.URL, QueueSize: 1, SpoolBytes: 1 << 20}
	m := &Metrics{}
	tel, err := NewTelemetry(c, m)
	if err != nil {
		t.Fatal(err)
	}
	useTestTransport(tel.http)
	spoolN(t, tel, 3)

	tel.deliver(context.Background())
	if m.SpoolCount.Load() != 0 {
		t.Fatalf("SpoolCount = %d; refused events would be resent every tick forever", m.SpoolCount.Load())
	}
	if m.Dropped.Load() != 2 {
		t.Errorf("Dropped = %d, want 2; a discarded event must be counted", m.Dropped.Load())
	}
	// A second tick has nothing left to do, which is the property that matters:
	// the queue drained rather than wedging behind what cannot be accepted.
	tel.deliver(context.Background())
	if requests.Load() != 1 {
		t.Errorf("requests = %d, want 1; the spool did not drain", requests.Load())
	}
}

// A malformed event that the control plane will never accept must not wedge the
// queue behind it. The rest of the batch has to make progress.
func TestOneRejectedEventDoesNotBlockTheRest(t *testing.T) {
	var rejected string
	cp := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []Event `json:"events"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		ids := []string{}
		for _, e := range body.Events {
			if e.ID == rejected {
				continue
			}
			ids = append(ids, e.ID)
		}
		w.Write(jsonBytes(map[string]any{"accepted": ids}))
	}))
	defer cp.Close()
	c := Config{DataDir: t.TempDir(), ControlURL: cp.URL, QueueSize: 1, SpoolBytes: 1 << 20}
	m := &Metrics{}
	tel, err := NewTelemetry(c, m)
	if err != nil {
		t.Fatal(err)
	}
	useTestTransport(tel.http)
	ids := spoolN(t, tel, 5)
	rejected = ids[2]

	tel.deliver(context.Background())
	if m.SpoolCount.Load() != 0 {
		t.Fatalf("SpoolCount = %d; the queue did not drain past the rejected event", m.SpoolCount.Load())
	}
	if m.Dropped.Load() != 1 {
		t.Errorf("Dropped = %d, want 1; the rejected event should be counted, not silently gone",
			m.Dropped.Load())
	}
}

// A control plane without the batch route must not stall delivery. This is what
// makes the change deployable in either order.
func TestFallsBackWhenTheBatchRouteIsMissing(t *testing.T) {
	var batchCalls, singleCalls atomic.Int64
	cp := testHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/batch") {
			batchCalls.Add(1)
			w.WriteHeader(404)
			return
		}
		singleCalls.Add(1)
		var e Event
		json.NewDecoder(r.Body).Decode(&e)
		w.Write(jsonBytes(map[string]string{"id": e.ID}))
	}))
	defer cp.Close()
	c := Config{DataDir: t.TempDir(), ControlURL: cp.URL, QueueSize: 1, SpoolBytes: 1 << 20}
	m := &Metrics{}
	tel, err := NewTelemetry(c, m)
	if err != nil {
		t.Fatal(err)
	}
	useTestTransport(tel.http)
	spoolN(t, tel, 4)

	tel.deliver(context.Background())
	if m.SpoolCount.Load() != 0 {
		t.Fatalf("SpoolCount = %d; fallback did not deliver everything", m.SpoolCount.Load())
	}
	if singleCalls.Load() != 4 {
		t.Errorf("per-event calls = %d, want 4", singleCalls.Load())
	}
	// Decided once, not re-probed every tick.
	spoolN(t, tel, 2)
	tel.deliver(context.Background())
	if batchCalls.Load() != 1 {
		t.Errorf("batch route probed %d times; the fallback should be remembered", batchCalls.Load())
	}
}

// Without configurable headers, OTLP export could only reach an unauthenticated
// collector on loopback. Honeycomb wants x-honeycomb-team, Grafana Cloud wants
// Basic auth, Datadog wants dd-api-key, and none were reachable.
func TestOTLPHeadersReachBothExporters(t *testing.T) {
	t.Setenv("HONEYCOMB_API_KEY", "hcaik_example")
	var metricsKey, tracesKey atomic.Value
	metricsKey.Store("")
	tracesKey.Store("")
	metrics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metricsKey.Store(r.Header.Get("x-honeycomb-team"))
		w.WriteHeader(200)
	}))
	defer metrics.Close()
	traces := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tracesKey.Store(r.Header.Get("x-honeycomb-team"))
		w.WriteHeader(200)
	}))
	defer traces.Close()

	c := Config{
		OTLPMetricsURL: metrics.URL, OTLPURL: traces.URL,
		OTLPHeaders: map[string]string{"x-honeycomb-team": "HONEYCOMB_API_KEY"},
	}
	tel := &Telemetry{c: c, m: &Metrics{}, http: metrics.Client(), start: time.Now()}
	tel.exportMetrics(context.Background())
	tel.exportOTLP(context.Background(), Event{ID: "a", Start: 1, End: 2})

	if metricsKey.Load() != "hcaik_example" {
		t.Errorf("metrics export sent %q; the backend would reject it", metricsKey.Load())
	}
	if tracesKey.Load() != "hcaik_example" {
		t.Errorf("traces export sent %q", tracesKey.Load())
	}
}

// A header value is read from the environment at request time, so an empty
// variable must not send an empty header that a backend would reject as
// malformed rather than as unauthenticated.
func TestOTLPHeaderWithEmptyEnvIsNotSent(t *testing.T) {
	var present atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, ok := r.Header["X-Honeycomb-Team"]
		present.Store(ok)
		w.WriteHeader(200)
	}))
	defer srv.Close()
	tel := &Telemetry{
		c:    Config{OTLPMetricsURL: srv.URL, OTLPHeaders: map[string]string{"x-honeycomb-team": "UNSET_VAR_NAME"}},
		m:    &Metrics{},
		http: srv.Client(), start: time.Now(),
	}
	tel.exportMetrics(context.Background())
	if present.Load() {
		t.Error("an empty environment variable produced an empty header")
	}
}

// spanAttrs decodes one exported span's attributes into a name/value map. Only
// the two value kinds these spans use are handled; anything else would be a new
// attribute type and should fail loudly here rather than read as absent.
func spanAttrs(t *testing.T, body []byte) (map[string]string, map[string]any) {
	t.Helper()
	var b struct {
		ResourceSpans []struct {
			ScopeSpans []struct {
				Spans []map[string]any `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	if err := json.Unmarshal(body, &b); err != nil {
		t.Fatalf("not valid OTLP JSON: %v", err)
	}
	if len(b.ResourceSpans) == 0 || len(b.ResourceSpans[0].ScopeSpans) == 0 ||
		len(b.ResourceSpans[0].ScopeSpans[0].Spans) == 0 {
		t.Fatalf("no span exported: %s", body)
	}
	span := b.ResourceSpans[0].ScopeSpans[0].Spans[0]
	out := map[string]string{}
	for _, a := range span["attributes"].([]any) {
		m := a.(map[string]any)
		v := m["value"].(map[string]any)
		switch {
		case v["stringValue"] != nil:
			out[m["key"].(string)] = v["stringValue"].(string)
		case v["intValue"] != nil:
			out[m["key"].(string)] = v["intValue"].(string)
		default:
			t.Fatalf("attribute %q has an unhandled value kind: %v", m["key"], v)
		}
	}
	return out, span
}

// Honeycomb's error-rate detection reads error.type, error.message,
// exception.type and exception.message. It does not read the span status, so a
// span carrying a correct status and nothing else leaves the service reported as
// having no recognised error attributes and refused monitoring entirely. That
// failure is silent: the data looks right, and the only symptom is a service
// that never gets watched.
func TestFailedSpanCarriesErrorType(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   string
	}{
		{"server error", 503, "503"},
		{"client error", 429, "429"},
		{"success", 200, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []byte
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got, _ = io.ReadAll(r.Body)
				w.Write([]byte(`{}`))
			}))
			defer srv.Close()
			tel := &Telemetry{c: Config{OTLPURL: srv.URL}, m: &Metrics{}, http: srv.Client()}
			tel.exportOTLP(context.Background(), Event{
				ID: "a", TraceID: "t", SpanID: "s", Status: tc.status, Start: 1, End: 2,
			})
			attrs, span := spanAttrs(t, got)
			if attrs["error.type"] != tc.want {
				t.Errorf("error.type = %q, want %q", attrs["error.type"], tc.want)
			}
			// The status block and the attribute have to agree, or one of the two
			// consumers of this span is being told the opposite of the other.
			if hasStatus := span["status"] != nil; hasStatus != (tc.want != "") {
				t.Errorf("span status present = %v, but error.type = %q", hasStatus, attrs["error.type"])
			}
		})
	}
}

// The routing loop knows which model answered, how many providers it took, and
// why any of them refused. Until these were exported the span kept none of it,
// so the metrics could say how often failover happened while no trace could say
// what happened to one request.
func TestSpanCarriesRoutingDecision(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	tel := &Telemetry{c: Config{OTLPURL: srv.URL}, m: &Metrics{}, http: srv.Client()}
	// A request that succeeded on its third provider after an account refusal:
	// status 200 with a fault set is not a contradiction, it is the case worth
	// being able to see.
	tel.exportOTLP(context.Background(), Event{
		ID: "a", TraceID: "t", SpanID: "s", Provider: "bedrock", Model: "claude-sonnet-4",
		Status: 200, Attempts: 3, Fault: faultAccount.String(), Start: 1, End: 2,
	})
	attrs, _ := spanAttrs(t, got)
	for k, want := range map[string]string{
		"gen_ai.provider.name": "aws.bedrock",
		"gen_ai.request.model": "claude-sonnet-4",
		"switchboard.attempts": "3",
		"switchboard.fault":    "account",
	} {
		if attrs[k] != want {
			t.Errorf("%s = %q, want %q", k, attrs[k], want)
		}
	}
}

// gen_ai.provider.name is a closed enum, and an off-enum value fails silently:
// nothing rejects it, the span still exports, and the traffic simply stops
// grouping with everything else that provider serves. Two of our four were
// wrong that way for as long as spans have existed.
//
// Asserted per provider rather than by round-tripping the same table the code
// reads, which would pass no matter what the table said. These four strings are
// copied from the registry by hand on purpose; that is the whole test.
func TestProviderNamesMatchSemconvEnum(t *testing.T) {
	for _, c := range []struct{ provider, wantProvider, wantOperation string }{
		{"openai", "openai", "chat"},
		{"anthropic", "anthropic", "chat"},
		// generativelanguage.googleapis.com, per the registry footnote scoping
		// gcp.gemini to that endpoint; gcp.vertex_ai is aiplatform.googleapis.com,
		// which is not the API adapter.go calls.
		{"gemini", "gcp.gemini", "generate_content"},
		// Converse is a chat API, so the operation is chat and not a bedrock-
		// specific value -- aws-bedrock.md defines no override.
		{"bedrock", "aws.bedrock", "chat"},
	} {
		t.Run(c.provider, func(t *testing.T) {
			var got []byte
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got, _ = io.ReadAll(r.Body)
				w.Write([]byte(`{}`))
			}))
			defer srv.Close()
			tel := &Telemetry{c: Config{OTLPURL: srv.URL}, m: &Metrics{}, http: srv.Client()}
			tel.exportOTLP(context.Background(), Event{
				ID: "a", TraceID: "t", SpanID: "s", Provider: c.provider,
				Model: "m", Status: 200, Start: 1, End: 2,
			})
			attrs, span := spanAttrs(t, got)
			if attrs["gen_ai.provider.name"] != c.wantProvider {
				t.Errorf("gen_ai.provider.name = %q, want %q", attrs["gen_ai.provider.name"], c.wantProvider)
			}
			if attrs["gen_ai.operation.name"] != c.wantOperation {
				t.Errorf("gen_ai.operation.name = %q, want %q", attrs["gen_ai.operation.name"], c.wantOperation)
			}
			// "{gen_ai.operation.name} {gen_ai.request.model}".
			if want := c.wantOperation + " m"; span["name"] != want {
				t.Errorf("span name = %q, want %q", span["name"], want)
			}
		})
	}
}

// A request refused before any route was chosen carries no model, and the span
// name is the operation alone rather than one with a trailing space.
func TestSpanNameWithoutModel(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	tel := &Telemetry{c: Config{OTLPURL: srv.URL}, m: &Metrics{}, http: srv.Client()}
	tel.exportOTLP(context.Background(), Event{
		ID: "a", TraceID: "t", SpanID: "s", Provider: "openai",
		Status: 403, Start: 1, End: 2,
	})
	if _, span := spanAttrs(t, got); span["name"] != "chat" {
		t.Errorf("span name = %q, want %q", span["name"], "chat")
	}
}

// The four names are a query surface: a dashboard or trigger filtering on
// "account" keeps working only while this mapping holds. Renaming one is a
// breaking change to anything built on it, so it should take a deliberate edit
// here rather than happening as a side effect of touching the enum.
func TestFaultNamesAreStable(t *testing.T) {
	for f, want := range map[fault]string{
		faultTerminal:  "terminal",
		faultRateLimit: "rate_limit",
		faultAccount:   "account",
		faultDegraded:  "degraded",
		fault(99):      "unknown",
	} {
		if got := f.String(); got != want {
			t.Errorf("fault(%d).String() = %q, want %q", int(f), got, want)
		}
	}
}

// Model and Fault were span-only because the control plane rejected any key its
// model did not name, so the fault classification that chose a route could not
// reach the store that replay reads. They travel in ext now, which exists so a
// gateway newer than its control plane degrades instead of going silent.
func TestEventCarriesModelAndFaultInExt(t *testing.T) {
	e := Event{ID: "a", RequestID: "r", Provider: "anthropic",
		Model: "claude-haiku-4-5-20251001", Fault: faultAccount.String()}

	var got map[string]any
	if err := json.Unmarshal(jsonBytes(e.wire()), &got); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	ext, ok := got["ext"].(map[string]any)
	if !ok {
		t.Fatalf("no ext object: %v", got)
	}
	if ext["model"] != "claude-haiku-4-5-20251001" || ext["fault"] != "account" {
		t.Errorf("ext = %v, want the model and fault", ext)
	}
	// Not at the top level: the core schema is the part both halves must agree
	// on, and it grows only when they can be released together.
	if _, top := got["model"]; top {
		t.Error("model leaked into the core schema")
	}
	if _, top := got["fault"]; top {
		t.Error("fault leaked into the core schema")
	}
}

// An event with nothing extra must not grow an empty object, so the common case
// stays exactly the bytes it was.
func TestEventWithoutExtrasHasNoExt(t *testing.T) {
	var got map[string]any
	if err := json.Unmarshal(jsonBytes(Event{ID: "a", RequestID: "r"}.wire()), &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["ext"]; ok {
		t.Error("an event with no extras emitted an ext object")
	}
}

// The spool path was batched because one POST per event is round-trip bound --
// "roughly twenty per second across a network at 50 ms" in its own comment --
// and the OTLP path was left in exactly that shape while being the one the
// deployment docs point at a hosted backend. A single goroutine doing a
// blocking POST per span caps how much tracing a gateway can emit, and the
// excess is dropped at Emit.
func TestOTLPSpansAreBatchedIntoOneRequest(t *testing.T) {
	var requests atomic.Int64
	var spans atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		b, _ := io.ReadAll(r.Body)
		var body struct {
			ResourceSpans []struct {
				ScopeSpans []struct {
					Spans []map[string]any `json:"spans"`
				} `json:"scopeSpans"`
			} `json:"resourceSpans"`
		}
		if json.Unmarshal(b, &body) == nil && len(body.ResourceSpans) > 0 &&
			len(body.ResourceSpans[0].ScopeSpans) > 0 {
			spans.Add(int64(len(body.ResourceSpans[0].ScopeSpans[0].Spans)))
		}
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	tel := &Telemetry{c: Config{OTLPURL: srv.URL}, m: &Metrics{}, http: srv.Client(),
		otlp: make(chan Event, 64)}
	// Queue several before the gatherer runs, so they are already waiting.
	const n = 12
	for i := 0; i < n; i++ {
		tel.otlp <- Event{ID: "e", TraceID: "t", SpanID: "s", Status: 200, Start: 1, End: 2}
	}
	first := <-tel.otlp
	tel.exportOTLPBatch(context.Background(), tel.gatherSpans(context.Background(), first))

	if got := spans.Load(); got != n {
		t.Errorf("delivered %d spans, want %d; none may be dropped by batching", got, n)
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("used %d requests for %d spans, want 1", got, n)
	}
}

// A quiet gateway must not hold a span waiting for a batch that will not fill.
func TestOTLPBatchFlushesWhenIdle(t *testing.T) {
	tel := &Telemetry{c: Config{}, m: &Metrics{}, otlp: make(chan Event)}
	start := time.Now()
	got := tel.gatherSpans(context.Background(), Event{ID: "only"})
	if len(got) != 1 {
		t.Errorf("gathered %d spans from an idle channel, want 1", len(got))
	}
	if elapsed := time.Since(start); elapsed > 2*otlpFlush {
		t.Errorf("waited %v for a batch that could not fill; flush is %v", elapsed, otlpFlush)
	}
}

// One span per request must still produce the shape the single-span assertions
// elsewhere depend on, since spanOf is now shared by both paths.
func TestSingleSpanExportStillWorks(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	tel := &Telemetry{c: Config{OTLPURL: srv.URL}, m: &Metrics{}, http: srv.Client()}
	tel.exportOTLP(context.Background(), Event{ID: "a", TraceID: "t", SpanID: "s",
		Provider: "openai", Status: 503, Start: 1, End: 2})

	attrs, _ := spanAttrs(t, got)
	if attrs["error.type"] != "503" || attrs["gen_ai.provider.name"] != "openai" {
		t.Errorf("single-span export lost attributes: %v", attrs)
	}
}

// adapter.go has always normalised four incompatible usage shapes into one set
// of counts, and then used them for a single integrity check and dropped them.
// Nothing downstream could say what a request cost, which is the first question
// anyone puts to a gateway.
func TestSpanCarriesTokenUsage(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	tel := &Telemetry{c: Config{OTLPURL: srv.URL}, m: &Metrics{}, http: srv.Client()}
	tel.exportOTLP(context.Background(), Event{
		ID: "a", TraceID: "t", SpanID: "s", Provider: "anthropic", Model: "claude-haiku-4-5",
		Status: 200, Start: 1, End: 2,
		// Anthropic's worked example: a 50-token message against a 100,000-token
		// warm cache. Input is the total; the parts are inside it.
		Usage: tokenUsage{Input: 100050, Output: 7, CacheRead: 100000},
	})
	attrs, _ := spanAttrs(t, got)
	for k, want := range map[string]string{
		"gen_ai.usage.input_tokens":            "100050",
		"gen_ai.usage.output_tokens":           "7",
		"gen_ai.usage.cache_read.input_tokens": "100000",
	} {
		if attrs[k] != want {
			t.Errorf("%s = %q, want %q", k, attrs[k], want)
		}
	}
	// Not measured is not the same as measured zero, and this provider wrote no
	// cache and did no reasoning on this call.
	for _, k := range []string{
		"gen_ai.usage.cache_write.input_tokens",
		"gen_ai.usage.reasoning.output_tokens",
	} {
		if _, ok := attrs[k]; ok {
			t.Errorf("%s was emitted for a call that did not measure it", k)
		}
	}
}

// A request that never reached a provider has no usage to report, and a span
// claiming it cost zero tokens is a false measurement rather than a missing one.
func TestSpanOmitsUsageWhenNoProviderAnswered(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	tel := &Telemetry{c: Config{OTLPURL: srv.URL}, m: &Metrics{}, http: srv.Client()}
	tel.exportOTLP(context.Background(), Event{
		ID: "a", TraceID: "t", SpanID: "s", Provider: "openai",
		Status: 403, Start: 1, End: 2,
	})
	attrs, _ := spanAttrs(t, got)
	for k := range attrs {
		if strings.HasPrefix(k, "gen_ai.usage.") {
			t.Errorf("%s emitted for a request that reached no provider", k)
		}
	}
}

// An output of zero IS a measurement: a reasoning model can spend its whole
// budget on hidden reasoning and write nothing, billed in full. That is the case
// this gateway exists to notice, so it must not be suppressed as "empty".
func TestZeroOutputTokensIsReportedNotSuppressed(t *testing.T) {
	u := tokenUsage{Input: 1024, Output: 0, Reasoning: 1024}
	got := map[string]int{}
	for _, a := range u.attrs() {
		got[a.key] = a.value
	}
	if v, ok := got["gen_ai.usage.output_tokens"]; !ok || v != 0 {
		t.Errorf("output_tokens = %v (present=%v), want 0 present", v, ok)
	}
	if got["gen_ai.usage.reasoning.output_tokens"] != 1024 {
		t.Errorf("reasoning = %d, want 1024", got["gen_ai.usage.reasoning.output_tokens"])
	}
}

// The control plane forbids unknown keys on the core event model, so usage
// travels in ext -- the same route model and fault take. A gateway newer than
// its control plane then degrades instead of having every event rejected.
func TestUsageReachesControlPlaneThroughExt(t *testing.T) {
	e := Event{ID: "a", Provider: "openai", Model: "gpt-5-nano",
		Usage: tokenUsage{Input: 11, Output: 7, Reasoning: 4}}.wire()
	for k, want := range map[string]any{
		"gen_ai.usage.input_tokens":            11,
		"gen_ai.usage.output_tokens":           7,
		"gen_ai.usage.reasoning.output_tokens": 4,
		"model":                                "gpt-5-nano",
	} {
		if e.Ext[k] != want {
			t.Errorf("ext[%q] = %v, want %v", k, e.Ext[k], want)
		}
	}
	// The core is unchanged: anything the control plane validates strictly must
	// still be exactly what it was.
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var core map[string]any
	json.Unmarshal(b, &core)
	if _, ok := core["usage"]; ok {
		t.Error("usage leaked into the core schema, which is extra=forbid")
	}
}
