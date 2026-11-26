package slogx

import (
	"errors"
	"hash/maphash"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Sampling keeps a burst of lines and then a fraction: per key and Tick,
// the first First lines pass, then every Thereafter-th (zero: none). The
// next line that passes carries suppressed=<count>. Zero Tick is one
// minute. Def.Limit is the special case First 1, Thereafter 0.
//
// Under pressure, new keys with the same policy share a shard's overflow
// counter. Overflow counters have the same bounded budget as regular keys.
// Expired counters and excess policy classes contribute unreported losses to
// Logs.UnattributedSuppressed.
type Sampling struct {
	First      int
	Thereafter int
	Tick       time.Duration
}

// limitWindow is the tick of Def.Limit and the default Sampling.Tick.
const limitWindow = time.Minute

func (s Sampling) validate() error {
	if s.First < 0 || s.Thereafter < 0 || s.Tick < 0 {
		return errors.New("slogx: sampling with a negative field")
	}
	return nil
}

func (s Sampling) tick() time.Duration {
	if s.Tick <= 0 {
		return limitWindow
	}
	return s.Tick
}

// maxSampleKeys bounds the keys a sampler tracks; see Sampling. The bound
// is split evenly between the shards.
const maxSampleKeys = 8192

// sampler counts per key. It starts as one shard under one lock. When lines
// are seen waiting for a shard's lock and the keys are many enough to spread
// (at least two per shard after the split), the shards double, up to the
// number of Ps; one hot key never splits, as more shards could not help it.
type sampler struct {
	set          atomic.Pointer[sampleSet]
	seed         maphash.Seed
	unattributed atomic.Uint64
	collisions   atomic.Uint32 // lock waits since the last growth
}

// sampleSet is one generation of shards. Growing locks every shard,
// copies the slots into the next generation and marks this one moved; a
// line that then gets a shard's lock sees moved and retries on the next.
type sampleSet struct {
	shards []sampleShard
	mask   uint64 // len(shards)-1
	perMax int    // keys per shard
	moved  bool   // written and read under every shard's lock
}

type sampleShard struct {
	mu        sync.Mutex
	seen      map[sampleKey]*sampleSlot
	lastSweep time.Time
}

// sampleKey identifies what a sampler counts: an event and the value of its
// Limit field, or a plain line's module, level and message. It is built
// from the line's own strings, without copying them.
type sampleKey struct {
	policy   Sampling
	a, b     string // event and field value, or module and message
	level    slog.Level
	plain    bool // a line without an event
	field    bool // the event's Limit field was present
	overflow bool // the shared key of a full shard
}

type sampleSlot struct {
	start      time.Time
	tick       time.Duration
	n          int
	suppressed uint64
}

func newSampleSet(k uint) *sampleSet {
	n := 1 << k
	return &sampleSet{shards: make([]sampleShard, n), mask: 1<<k - 1, perMax: max(maxSampleKeys/n, 1)}
}

// newSamplerN makes a sampler that starts with 1<<k shards.
func newSamplerN(k uint) *sampler {
	sm := &sampler{seed: maphash.MakeSeed()}
	sm.set.Store(newSampleSet(k))
	return sm
}

// lock returns the shard of key, locked, in the current generation.
func (sm *sampler) lock(key sampleKey) *sampleShard {
	h := maphash.Comparable(sm.seed, key)
	for {
		set := sm.set.Load()
		sh := &set.shards[h&set.mask]
		if !sh.mu.TryLock() {
			if sm.collisions.Add(1) >= growAfter {
				sm.collisions.Store(0)
				sm.grow(set)
				continue
			}
			sh.mu.Lock()
		}
		if !set.moved {
			return sh
		}
		sh.mu.Unlock()
	}
}

// grow replaces set by one of twice the shards, if set is still current
// and its keys are enough to spread.
func (sm *sampler) grow(set *sampleSet) {
	if len(set.shards) >= maxStripes() {
		return
	}
	for i := range set.shards {
		set.shards[i].mu.Lock()
	}
	defer func() {
		for i := range set.shards {
			set.shards[i].mu.Unlock()
		}
	}()
	sm.growLocked(set)
}

// growLocked is grow with every shard of set locked.
func (sm *sampler) growLocked(set *sampleSet) {
	keys := 0
	for i := range set.shards {
		keys += len(set.shards[i].seen)
	}
	if set.moved || sm.set.Load() != set || keys < 4*len(set.shards) {
		return
	}
	k := uint(0)
	for 1<<k < 2*len(set.shards) {
		k++
	}
	next := newSampleSet(k)
	for i := range set.shards {
		for key, sl := range set.shards[i].seen {
			dst := &next.shards[maphash.Comparable(sm.seed, key)&next.mask]
			if dst.seen == nil {
				dst.seen = map[sampleKey]*sampleSlot{}
			}
			if old, ok := dst.seen[key]; ok {
				// only the overflow key is in several old shards: merge
				// so no suppressed line goes unreported
				old.suppressed += sl.suppressed
				old.n = max(old.n, sl.n)
				if sl.start.Before(old.start) {
					old.start = sl.start
				}
				continue
			}
			dst.seen[key] = sl
		}
	}
	sm.set.Store(next)
	set.moved = true
}

// allow reports whether the line of key passes s at now, and how many
// lines of key were suppressed before it.
func (sm *sampler) allow(key sampleKey, s Sampling, now time.Time) (uint64, bool) {
	tick := s.tick()
	s.Tick = tick
	key.policy = s
	sh := sm.lock(key)
	defer sh.mu.Unlock()
	perMax := sm.set.Load().perMax
	if sh.seen == nil {
		sh.seen = map[sampleKey]*sampleSlot{}
	}
	sl, ok := sh.seen[key]
	if !ok {
		if len(sh.seen) >= perMax {
			sm.sweep(sh, now)
		}
		if len(sh.seen) >= perMax {
			key = sampleKey{overflow: true, policy: s}
			sl = sh.seen[key]
			if sl == nil && len(sh.seen) >= 2*perMax {
				sm.unattributed.Add(1)
				return 0, false
			}
		}
		if sl == nil {
			sl = &sampleSlot{start: now, tick: tick}
			sh.seen[key] = sl
		}
	}
	if now.Sub(sl.start) >= sl.tick {
		sl.start, sl.n, sl.tick = now, 0, tick
	}
	sl.n++
	pass := sl.n <= s.First || (s.Thereafter > 0 && (sl.n-s.First)%s.Thereafter == 0)
	if !pass {
		sl.suppressed++
		return 0, false
	}
	n := sl.suppressed
	sl.suppressed = 0
	return n, true
}

// sweep drops the slots whose tick has passed, at most once per second so
// a full shard costs one scan per second, not one per line. A dropped
// slot contributes its unreported count to the aggregate.
func (sm *sampler) sweep(sh *sampleShard, now time.Time) {
	if now.Sub(sh.lastSweep) < time.Second {
		return
	}
	sh.lastSweep = now
	for k, v := range sh.seen {
		if now.Sub(v.start) >= v.tick {
			sm.unattributed.Add(v.suppressed)
			delete(sh.seen, k)
		}
	}
}

// keyOf is the sample key of r: the event and the value of its Limit
// field, or for plain lines the module, level and message.
func keyOf(r *Record, field string) sampleKey {
	if r.Def != nil {
		k := sampleKey{a: r.Event}
		if field != "" && field != "*" {
			v, ok := r.Attr(r.group + field)
			if !ok && r.group != "" {
				v, ok = r.Attr(field)
			}
			if ok {
				k.b, k.field = v.String(), true
			}
		}
		return k
	}
	return sampleKey{a: r.Module, b: r.Msg, level: r.Level, plain: true}
}
