// Package storeforward implements EDGE-130's store-and-forward durable
// queue: a size- and time-bounded FIFO backed by local disk, so a node
// writing to a remote destination (a central broker/database, a bus-out
// link to another runtime) can keep accepting datagrams while that
// destination is unreachable and drain them, in order, once it comes back
// — without needing the control plane or any other process to be involved.
// Like every other bounded queue in this codebase (BUS-110), nothing here
// buffers unboundedly: once MaxSizeBytes or MaxAge is exceeded, the oldest
// entries are dropped and counted.
package storeforward

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// entryFile is the on-disk shape of one queued datagram. Payload is
// whatever the caller enqueued (typically a JSON-encoded datagram);
// base64-wrapping it keeps the file itself simple JSON regardless of
// payload content.
type entryFile struct {
	EnqueuedAtUnixNano int64  `json:"enqueuedAtUnixNano"`
	Payload            string `json:"payload"`
}

// Entry is one queued item, returned by Peek.
type Entry struct {
	ID         string
	Payload    []byte
	EnqueuedAt time.Time
}

// Queue is a durable, disk-backed FIFO. Safe for concurrent use.
type Queue struct {
	dir          string
	maxSizeBytes int64
	maxAge       time.Duration

	mu      sync.Mutex
	nextSeq int64
	// order holds ids (filenames without extension) oldest-first; sizes
	// tracks each id's on-disk byte size for the size bound.
	order []string
	sizes map[string]int64
	total int64
}

// Open creates dir if needed and rebuilds the queue's index from whatever
// entry files are already there (a restart-surviving durable queue must
// reconstruct its own state purely from disk — there is no separate index
// file to go stale or get lost).
func Open(dir string, maxSizeBytes int64, maxAge time.Duration) (*Queue, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storeforward: creating %s: %w", dir, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("storeforward: reading %s: %w", dir, err)
	}

	q := &Queue{dir: dir, maxSizeBytes: maxSizeBytes, maxAge: maxAge, sizes: make(map[string]int64)}

	type idSeq struct {
		id  string
		seq int64
	}
	var ids []idSeq
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		seq, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			continue // not one of ours, ignore
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		ids = append(ids, idSeq{id: id, seq: seq})
		q.sizes[id] = info.Size()
		q.total += info.Size()
		if seq >= q.nextSeq {
			q.nextSeq = seq + 1
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].seq < ids[j].seq })
	for _, e := range ids {
		q.order = append(q.order, e.id)
	}
	return q, nil
}

// Enqueue durably appends payload (enqueuedAt is normally time.Now(), a
// parameter so tests can control it and so a re-enqueue can preserve an
// original timestamp) and then applies the age/size bounds, dropping the
// oldest entries as needed. Returns how many entries (not counting the one
// just added) were dropped by this call, for BUS-110 drop metrics.
func (q *Queue) Enqueue(payload []byte, enqueuedAt time.Time) (dropped int, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	id := fmt.Sprintf("%020d", q.nextSeq)
	q.nextSeq++

	ef := entryFile{EnqueuedAtUnixNano: enqueuedAt.UnixNano(), Payload: base64.StdEncoding.EncodeToString(payload)}
	data, err := json.Marshal(ef)
	if err != nil {
		return 0, fmt.Errorf("storeforward: marshaling entry: %w", err)
	}

	path := filepath.Join(q.dir, id+".json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return 0, fmt.Errorf("storeforward: writing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return 0, fmt.Errorf("storeforward: renaming %s: %w", tmp, err)
	}

	q.order = append(q.order, id)
	q.sizes[id] = int64(len(data))
	q.total += int64(len(data))

	dropped += q.dropExpiredLocked(enqueuedAt)
	dropped += q.dropOverflowLocked()
	return dropped, nil
}

// dropExpiredLocked removes entries older than MaxAge (0 = no age bound),
// measured against "now" (normally the just-enqueued item's own
// timestamp, so tests can drive time deterministically).
func (q *Queue) dropExpiredLocked(now time.Time) int {
	if q.maxAge <= 0 {
		return 0
	}
	dropped := 0
	for len(q.order) > 0 {
		id := q.order[0]
		ef, err := q.readLocked(id)
		if err != nil {
			// Corrupt/unreadable entry: drop it, it can never be drained anyway.
			q.removeLocked(id)
			dropped++
			continue
		}
		if now.Sub(time.Unix(0, ef.EnqueuedAtUnixNano)) <= q.maxAge {
			break
		}
		q.removeLocked(id)
		dropped++
	}
	return dropped
}

// dropOverflowLocked drops the oldest entries until the queue fits within
// MaxSizeBytes (0 = no size bound) — BUS-110 "drop oldest" overflow policy.
func (q *Queue) dropOverflowLocked() int {
	if q.maxSizeBytes <= 0 {
		return 0
	}
	dropped := 0
	for q.total > q.maxSizeBytes && len(q.order) > 0 {
		q.removeLocked(q.order[0])
		dropped++
	}
	return dropped
}

func (q *Queue) removeLocked(id string) {
	_ = os.Remove(filepath.Join(q.dir, id+".json"))
	q.total -= q.sizes[id]
	delete(q.sizes, id)
	if len(q.order) > 0 && q.order[0] == id {
		q.order = q.order[1:]
		return
	}
	for i, o := range q.order {
		if o == id {
			q.order = append(q.order[:i], q.order[i+1:]...)
			return
		}
	}
}

func (q *Queue) readLocked(id string) (entryFile, error) {
	data, err := os.ReadFile(filepath.Join(q.dir, id+".json"))
	if err != nil {
		return entryFile{}, err
	}
	var ef entryFile
	if err := json.Unmarshal(data, &ef); err != nil {
		return entryFile{}, err
	}
	return ef, nil
}

// Peek returns the oldest entry without removing it, or ok == false if the
// queue is empty.
func (q *Queue) Peek() (Entry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.order) == 0 {
		return Entry{}, false
	}
	id := q.order[0]
	ef, err := q.readLocked(id)
	if err != nil {
		// Corrupt entry on disk: drop it so Drain doesn't spin forever on it.
		q.removeLocked(id)
		return Entry{}, false
	}
	payload, err := base64.StdEncoding.DecodeString(ef.Payload)
	if err != nil {
		q.removeLocked(id)
		return Entry{}, false
	}
	return Entry{ID: id, Payload: payload, EnqueuedAt: time.Unix(0, ef.EnqueuedAtUnixNano)}, true
}

// Remove deletes the given entry (called once it has been successfully
// delivered).
func (q *Queue) Remove(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.removeLocked(id)
}

// Len returns the number of currently queued entries.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order)
}

// SizeBytes returns the queue's current total on-disk size.
func (q *Queue) SizeBytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.total
}
