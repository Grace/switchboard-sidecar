package gateway

// Idempotency keys.
//
// Four paths in chat() end with the outcome of a generation genuinely unknown:
// a transport error after the request was sent leaves no way to tell whether the
// provider ran it. A caller who sees that must choose between paying twice and
// dropping a result they already paid for. This is what an idempotency key is
// for, and it is the question an enterprise review asks first.
//
// What this promises is bounded, and the bounds are the honest part:
//
//   - It survives a process restart, because entries are on disk in data_dir,
//     which the gateway already owns exclusively under a file lock.
//   - It does not survive task replacement. data_dir is ephemeral unless mounted
//     on EFS, exactly as docs/DEPLOYMENT.md already says of the policy cache and
//     the spool. A retry that lands on a replacement task is a new request.
//   - It expires. This is a cache with a stated window, not a permanent ledger.
//
// Exactly-once generation across provider APIs remains impossible. Nothing here
// changes that; it narrows the window in which the ambiguity can cost money.
//
// Entries hold request and response content, which is why the feature is off
// unless configured: a change to what sits at rest should not arrive silently in
// an upgrade.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type idemState string

const (
	// idemRunning: a request holds this key and has not finished.
	idemRunning idemState = "running"
	// idemDone: the response is stored and can be replayed.
	idemDone idemState = "done"
	// idemUnknown: the provider may or may not have generated. This is the state
	// the whole feature exists for, and the only one that is deliberately sticky:
	// releasing the key here would let a retry charge a second time for work that
	// may already have happened.
	idemUnknown idemState = "unknown"
)

type idemEntry struct {
	State    idemState `json:"state"`
	BodyHash string    `json:"body_hash"`
	Stored   int64     `json:"stored_unix"`
	Status   int       `json:"status,omitempty"`
	Stream   bool      `json:"stream,omitempty"`
	// Response is the completion JSON for a nonstreaming request, or the
	// assembled text of a stream. A replayed stream carries the same answer, not
	// the original timing; docs/API.md says so rather than letting a caller infer
	// byte-identical replay.
	Response json.RawMessage `json:"response,omitempty"`
	Text     string          `json:"text,omitempty"`
	Finish   string          `json:"finish,omitempty"`
}

// errIdemConflict is a key held by a request that is still running, or one whose
// outcome is unknown. errIdemMismatch is the same key with a different body,
// which is a client bug: answering it with the first response would be silently
// wrong.
var (
	errIdemConflict = errors.New("idempotency key is in use or its outcome is unknown")
	errIdemMismatch = errors.New("idempotency key was already used with a different request body")
)

// idemStore is a bounded, expiring, disk-backed map from key to outcome.
//
// One file per entry looks like the telemetry spool, whose per-request file
// churn was measured growing reclaimable kernel slab. It is not the same
// exposure: only requests that carry a key create entries, so the churn is
// proportional to idempotent traffic rather than to every request.
type idemStore struct {
	dir   string
	ttl   time.Duration
	limit int64
	m     *Metrics

	mu    sync.Mutex
	used  int64
	count int
}

func NewIdemStore(dir string, ttl time.Duration, limit int64, m *Metrics) (*idemStore, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	s := &idemStore{dir: dir, ttl: ttl, limit: limit, m: m}
	// Recover the byte count across a restart, and drop anything already expired
	// so a restart is also a compaction.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		i, err := e.Info()
		if err != nil {
			continue
		}
		if time.Since(i.ModTime()) > ttl {
			os.Remove(filepath.Join(dir, e.Name()))
			continue
		}
		s.used += i.Size()
		s.count++
	}
	return s, nil
}

// Sweep removes expired entries. Without it, expiry happened only in
// NewIdemStore, which meant it happened only at startup -- and the consequence
// was not merely stale files.
//
// finish() overwrites an entry and never deletes one; only release(), the
// un-settled path, decrements used. So used climbed monotonically toward the
// configured limit and stayed there. Past it, write() refuses every entry for a
// new key, begin() returns that error, and the switch in Server.chat matches
// only errIdemMismatch and errIdemConflict -- so a full store fell through
// unmatched and the request proceeded with no idempotency entry at all.
//
// From that point the feature was off while appearing healthy: replay, conflict
// and unknown all read zero, which looks like "no duplicates seen" rather than
// "no longer checking". An ambiguous retry then found nothing, was treated as
// fresh, and was billed a second time -- the exact failure this file exists to
// prevent. It never recovered without a restart.
//
// It also made docs/SECURITY.md's claim that entries expire with the TTL false.
func (s *idemStore) Sweep() {
	// Unlocked scan, bounded pass, mutex held only for the accounting. This held
	// s.mu across the whole ReadDir and an unbounded delete loop, once a minute,
	// while begin, finish, release and write contend on the same mutex from the
	// request path -- so every retry-bearing request queued behind a directory
	// walk. capture.go rejected exactly this pattern, naming it as inherited
	// from here; the fix travelled the other way at last.
	//
	// Deleting outside the lock is safe because the filesystem is the store of
	// record: a request that reads a file this sweep is about to remove gets the
	// entry it would have got a moment earlier, and one that arrives after gets
	// a miss, which is what an expired entry means.
	entries, _ := os.ReadDir(s.dir)
	type victim struct {
		name string
		size int64
	}
	var expired []victim
	for _, e := range entries {
		i, err := e.Info()
		if err != nil || time.Since(i.ModTime()) <= s.ttl {
			continue
		}
		expired = append(expired, victim{e.Name(), i.Size()})
		if len(expired) >= idemSweepPerPass {
			break
		}
	}
	for _, v := range expired {
		if os.Remove(filepath.Join(s.dir, v.name)) != nil {
			continue
		}
		s.mu.Lock()
		s.used -= v.size
		s.count--
		s.mu.Unlock()
	}
}

// idemSweepPerPass bounds one sweep, so a tick costs the same whether the
// directory holds ten entries or a million.
//
// Its own constant rather than capture.go's sweepPerPass, which is deliberate:
// the two directories are not the same shape. Capture writes a file for every
// request, including 401s and policy refusals; this one writes only for requests
// that carried an idempotency key. Sharing the number would imply it had been
// tuned for both, and the next person to change one would silently change the
// other.
const idemSweepPerPass = 2000

// path derives a filename from the key by hashing it. A key is caller-supplied
// and would otherwise be a path traversal straight into the data directory.
func (s *idemStore) path(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(s.dir, hex.EncodeToString(sum[:])+".json")
}

func hashBody(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// begin claims a key. It returns a replayable entry when one exists, or an error
// when the key is held, ambiguous, or bound to a different body.
func (s *idemStore) begin(key string, body []byte) (*idemEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e := s.read(key); e != nil {
		if e.BodyHash != hashBody(body) {
			s.m.IdempotentConflict.Add(1)
			return nil, errIdemMismatch
		}
		switch e.State {
		case idemDone:
			s.m.IdempotentReplay.Add(1)
			return e, nil
		case idemUnknown:
			s.m.IdempotentUnknown.Add(1)
			return nil, errIdemConflict
		default:
			s.m.IdempotentConflict.Add(1)
			return nil, errIdemConflict
		}
	}
	// Written before the provider is contacted, so a concurrent duplicate finds
	// it and is refused rather than both reaching the provider.
	return nil, s.write(key, &idemEntry{
		State: idemRunning, BodyHash: hashBody(body), Stored: time.Now().Unix(),
	})
}

// finish records a terminal outcome. Releasing a key is as important as holding
// one: an unambiguous failure such as a provider 400 must not burn the key,
// because the caller can fix the request and retry.
func (s *idemStore) finish(key string, e *idemEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.write(key, e)
}

func (s *idemStore) release(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fi, err := os.Stat(s.path(key)); err == nil {
		s.used -= fi.Size()
		s.count--
	}
	os.Remove(s.path(key))
}

func (s *idemStore) read(key string) *idemEntry {
	b, err := os.ReadFile(s.path(key))
	if err != nil {
		return nil
	}
	var e idemEntry
	if json.Unmarshal(b, &e) != nil {
		return nil
	}
	if time.Since(time.Unix(e.Stored, 0)) > s.ttl {
		return nil
	}
	return &e
}

// write bounds the store the way the telemetry spool does: refuse to store
// rather than grow without limit. A refused entry is not an error to the caller,
// who simply gets no idempotency protection for that request, which is strictly
// what they had before the feature existed.
func (s *idemStore) write(key string, e *idemEntry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	existing := int64(0)
	if fi, statErr := os.Stat(s.path(key)); statErr == nil {
		existing = fi.Size()
	} else if s.used+int64(len(b)) > s.limit {
		// A full store means new keys are refused, and Server.chat does not
		// match that error -- so the request proceeds with no entry and
		// idempotency is silently off. Its own counter, because that is a
		// capacity decision rather than a disk fault.
		s.m.IdemFull.Add(1)
		return errors.New("idempotency store is full")
	}
	if err := os.WriteFile(s.path(key), b, 0600); err != nil {
		s.m.DiskErrors.Add(1)
		return err
	}
	if existing == 0 {
		s.count++
	}
	s.used += int64(len(b)) - existing
	return nil
}
