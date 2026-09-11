package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

// Circuit state learned from the rest of the fleet.
//
// A gateway learns a provider is unhealthy by paying for it: three failures
// before the breaker opens, per process. Run four and each pays the same three
// failures for the same outage; autoscale and every new instance pays again for
// as long as it lasts. The knowledge exists, it just does not travel.
//
// Three properties make this safe to ship, and all three are easy to lose:
//
// Advisory, never authoritative. What arrives here produces a skip, checked
// beside the local circuit rather than folded into it. The local breaker stays
// the only thing that decides local health, so a wrong hint costs a skipped
// route and never corrupts the state this process learned itself.
//
// It expires faster than it is refreshed. Hints are dropped after one poll
// interval's grace, so a control plane that stops answering means a gateway
// stops hearing, its hints lapse, and it decides alone -- exactly what it did
// before this existed. That is the whole reason this is not a new dependency,
// and it stops being true the moment the TTL outlives the poll.
//
// It can never skip everything. If every eligible route is hinted against, none
// is: the same rule budget-aware routing already applies, for the same reason.
// Without it one bad report is a fleet-wide outage of the gateway's own making,
// which is a far worse failure than the one being avoided.
type fleetHealth struct {
	mu   sync.Mutex
	seen map[string]time.Time // provider -> when the hint stops applying
}

func newFleetHealth() *fleetHealth { return &fleetHealth{seen: map[string]time.Time{}} }

// apply replaces the hint set with what the control plane just said. A replace
// rather than a merge: the response is the fleet's current view, so a provider
// absent from it is a provider nobody is currently reporting, and keeping an old
// hint for it would be inventing evidence.
func (f *fleetHealth) apply(reports map[string]time.Duration, max time.Duration) {
	if f == nil {
		return
	}
	now := time.Now()
	next := make(map[string]time.Time, len(reports))
	for provider, d := range reports {
		// Bounded here as well as at the control plane. The reporter chose this
		// number and the reporter is another gateway.
		next[provider] = now.Add(min(d, max))
	}
	f.mu.Lock()
	f.seen = next
	f.mu.Unlock()
}

// unhealthy reports whether the fleet currently says to avoid this provider.
func (f *fleetHealth) unhealthy(provider string) bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	until, ok := f.seen[provider]
	return ok && time.Now().Before(until)
}

// fleetHintMax bounds how long one report can steer this process, whatever it
// asked for. Kept close to the sync interval so a hint cannot outlive the poll
// that would have refreshed or withdrawn it: if the control plane goes away, so
// does the hint, within one interval.
const fleetHintMax = 45 * time.Second

// shareable says whether a fault means anything to another gateway.
//
// This is the whole safety argument for the feature, so it is a function with a
// name rather than a condition inline at the call site.
//
//	account   - the provider says this tenant cannot pay. True for every gateway
//	            holding that key, and exactly what an external status page
//	            cannot see.
//	degraded  - a 5xx. Plausibly the provider, possibly this host's network,
//	            which is why the control plane makes it wait for a second
//	            instance to agree.
//	refused   - 401, 403, 404. One deployment's wrong key or wrong model name.
//	            Broadcasting it would take a provider away from a fleet it is
//	            serving perfectly well.
//	terminal  - 400, 422. About the request, not the provider.
//	rate_limit- this caller's quota, already handled by Retry-After and a
//	            cooldown, and sharing it would spread one tenant's burst.
func shareable(f fault) bool { return f == faultAccount || f == faultDegraded }

// fleetFor returns a store only when the operator asked for one. A nil store
// answers false to every question and ignores every update, so the feature being
// off cannot leave it half on.
func fleetFor(c Config) *fleetHealth {
	if !c.FleetHealth {
		return nil
	}
	return newFleetHealth()
}

// fleetSyncOnce exchanges circuit state with the rest of the tenant's fleet.
//
// Every failure here is silent and total: the hints this process already holds
// expire on their own, and it goes back to deciding alone. That is the point.
// There is no retry, no spool and no error surfaced to a caller, because none of
// those would be improvements -- a report that arrives late describes a provider
// that has probably recovered, and acting on it is worse than dropping it.
//
// This is the opposite of the telemetry path, which is durable, ordered and
// retried, and it is why the two do not share a transport despite both talking
// to the control plane.
func (s *Server) fleetSyncOnce(ctx context.Context) {
	if s.fleet == nil || s.C.ControlURL == "" {
		return
	}
	reports := []map[string]any{}
	for provider, f := range s.drainReports() {
		reports = append(reports, map[string]any{
			"provider": provider,
			"fault":    f.String(),
			// What this process has withheld the provider for. The control plane
			// bounds it again, because the sender is another gateway.
			"ttl_seconds": int(fleetHintMax / time.Second),
		})
	}
	body := map[string]any{"instance_id": s.instanceID, "reports": reports}

	req, err := http.NewRequestWithContext(ctx, "POST",
		trimURL(s.C.ControlURL)+"/v1/health", bytes.NewReader(jsonBytes(body)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+os.Getenv(s.C.ControlTokenEnv))
	res, err := s.HTTP.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return
	}
	var decoded struct {
		Unhealthy []struct {
			Provider string `json:"provider"`
			Fault    string `json:"fault"`
			Seconds  int    `json:"seconds"`
		} `json:"unhealthy"`
	}
	if json.NewDecoder(io.LimitReader(res.Body, 1<<16)).Decode(&decoded) != nil {
		return
	}
	next := map[string]time.Duration{}
	for _, u := range decoded.Unhealthy {
		if u.Seconds <= 0 {
			continue
		}
		next[u.Provider] = time.Duration(u.Seconds) * time.Second
	}
	s.fleet.apply(next, fleetHintMax)
	s.Metrics.FleetHints.Store(int64(len(next)))
}
