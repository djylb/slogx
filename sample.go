package slogx

import (
	"errors"
	"hash/maphash"
	"log/slog"
	"sync"
	"time"
)

// Sampling keeps a burst of lines and then a fraction: per key and Tick,
// the first First lines pass, then every Thereafter-th (zero: none). The
// next line that passes carries suppressed=<count>. Zero Tick is one
// minute. Def.Limit is the special case First 1, Thereafter 0.
//
// A sampler tracks up to 8192 keys, split evenly over its shards. Beyond
// a shard's share, lines of new keys in that shard share its overflow key,
// so a flood of distinct keys (such as source addresses an attacker picks)
// is limited as one key per shard.
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

// sampler counts per key. Its keys are spread over shards, each with its
// own lock, so lines of different keys rarely wait for each other.
type sampler struct {
	shards []sampleShard
	seed   maphash.Seed
	mask   uint64 // shard index bits: len(shards)-1
	perMax int    // keys per shard
}

type sampleShard struct {
	mu        sync.Mutex
	seen      map[sampleKey]*sampleSlot
	lastSweep time.Time
	_         [64]byte // keep neighbouring shards' locks apart
}

// sampleKey identifies what a sampler counts: an event and the value of its
// Limit field, or a plain line's module, level and message. It is built
// from the line's own strings, without copying them.
type sampleKey struct {
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

func newSampler() sampler { return newSamplerN(stripeShift()) }

// newSamplerN makes a sampler of 1<<k shards.
func newSamplerN(k uint) sampler {
	n := 1 << k
	return sampler{shards: make([]sampleShard, n), seed: maphash.MakeSeed(), mask: 1<<k - 1, perMax: max(maxSampleKeys/n, 1)}
}

// allow reports whether the line of key passes s at now, and how many
// lines of key were suppressed before it.
func (sm *sampler) allow(key sampleKey, s Sampling, now time.Time) (uint64, bool) {
	tick := s.tick()
	sh := &sm.shards[maphash.Comparable(sm.seed, key)&sm.mask]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.seen == nil {
		sh.seen = map[sampleKey]*sampleSlot{}
	}
	sl, ok := sh.seen[key]
	if !ok {
		if len(sh.seen) >= sm.perMax {
			sh.sweep(now)
		}
		if len(sh.seen) >= sm.perMax {
			key = sampleKey{overflow: true}
			sl = sh.seen[key]
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
// slot's suppressed count is lost; it was never followed by a line.
func (sh *sampleShard) sweep(now time.Time) {
	if now.Sub(sh.lastSweep) < time.Second {
		return
	}
	sh.lastSweep = now
	for k, v := range sh.seen {
		if now.Sub(v.start) >= v.tick {
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
			if v, ok := r.Attr(field); ok {
				k.b, k.field = v.String(), true
			}
		}
		return k
	}
	return sampleKey{a: r.Module, b: r.Msg, level: r.Level, plain: true}
}
