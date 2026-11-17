package slogx

import (
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"time"
)

// inflight counts the lines using a pipeline, so that retiring it can wait
// for them without every line touching one shared word: a line counts
// itself in one of several cache-line-padded stripes, picked at random, and
// leaves the same one. A shared reader count (sync.RWMutex) bounces one
// cache line between all cores on every line.
//
// enter and close pair as in Dekker's algorithm: a line adds to its stripe,
// then checks closed; close sets closed, then reads the stripes. Go's
// atomics are sequentially consistent, so either the line sees closed and
// backs out, or close sees the line and waits for it.
type inflight struct {
	stripes []inflightStripe
	closed  atomic.Bool
}

type inflightStripe struct {
	n atomic.Int64
	_ [56]byte // own cache line
}

// stripeCount is GOMAXPROCS at the time a pipeline is made, rounded up to a
// power of two; more stripes than Ps buy nothing.
func stripeCount() int { return 1 << stripeShift() }

// stripeShift is the log2 of stripeCount.
func stripeShift() uint {
	k := uint(0)
	for 1<<k < runtime.GOMAXPROCS(0) {
		k++
	}
	return k
}

// enter counts a line in; it fails once the pipeline is closed.
func (f *inflight) enter() (uint32, bool) {
	slot := rand.Uint32() & uint32(len(f.stripes)-1) //nolint:gosec // spreading load, not security
	f.stripes[slot].n.Add(1)
	if f.closed.Load() {
		f.stripes[slot].n.Add(-1)
		return 0, false
	}
	return slot, true
}

// leave counts out the line that entered at slot.
func (f *inflight) leave(slot uint32) {
	f.stripes[slot].n.Add(-1)
}

// close stops new lines entering and waits until those in flight leave.
// Lines are short, so it polls, backing off to a millisecond, rather than
// making every leave check for a waiter.
func (f *inflight) close() {
	f.closed.Store(true)
	wait := time.Microsecond
	for f.busy() {
		if wait < 50*time.Microsecond {
			runtime.Gosched()
		} else {
			time.Sleep(wait)
		}
		wait = min(wait*2, time.Millisecond)
	}
}

func (f *inflight) busy() bool {
	var n int64
	for i := range f.stripes {
		n += f.stripes[i].n.Load()
	}
	return n != 0
}
