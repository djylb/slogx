package slogx

import (
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"time"
)

// inflight counts the lines using a pipeline, so that retiring it can wait
// for them. It starts as one counter; when lines are seen colliding on it
// (a failed compare-and-swap), it grows into cache-line-padded stripes that
// lines pick at random, doubling while collisions go on, up to the number
// of Ps. A quiet pipeline never leaves the single counter.
//
// Stripe sets are only ever added: each new set links the one before, and
// a line leaves the counter it entered. enter and close pair as in
// Dekker's algorithm: a line adds to a counter, then checks closed; close
// sets closed, then sums every set reachable from the newest. Go's atomics
// are sequentially consistent, so either the line sees closed and backs
// out, or close sees its count, in a set no newer than the head it loads.
type inflight struct {
	stripes    atomic.Pointer[stripeSet]
	n          atomic.Int64  // the counter before any stripes
	collisions atomic.Uint32 // failed swaps since the last growth
	closed     atomic.Bool
}

type stripeSet struct {
	prev *stripeSet
	c    []inflightStripe
}

type inflightStripe struct {
	n atomic.Int64
	_ [56]byte // own cache line
}

// growAfter is how many collisions make the stripes double.
const growAfter = 64

// maxStripes is the stripes worth having: one per P, rounded up to a power
// of two. More would only spread the same Ps thinner.
func maxStripes() int { return 1 << stripeShift() }

// stripeShift is the log2 of maxStripes.
func stripeShift() uint {
	k := uint(0)
	for 1<<k < runtime.GOMAXPROCS(0) {
		k++
	}
	return k
}

// enter counts a line in and returns the counter it must leave; it fails
// once the pipeline is closed.
func (f *inflight) enter() (*atomic.Int64, bool) {
	c := &f.n
	if s := f.stripes.Load(); s != nil {
		c = &s.c[rand.Uint32()&uint32(len(s.c)-1)].n //nolint:gosec // spreading load, not security
	}
	if v := c.Load(); !c.CompareAndSwap(v, v+1) {
		c.Add(1)
		f.collided()
	}
	if f.closed.Load() {
		c.Add(-1)
		return nil, false
	}
	return c, true
}

// leave counts out the line that entered on c.
func (f *inflight) leave(c *atomic.Int64) { c.Add(-1) }

// collided notes a collision and doubles the stripes after enough.
func (f *inflight) collided() {
	if f.collisions.Add(1) < growAfter {
		return
	}
	f.collisions.Store(0)
	cur := f.stripes.Load()
	n := 2
	if cur != nil {
		n = 2 * len(cur.c)
	}
	if n > maxStripes() {
		return
	}
	f.stripes.CompareAndSwap(cur, &stripeSet{prev: cur, c: make([]inflightStripe, n)})
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
	n := f.n.Load()
	for s := f.stripes.Load(); s != nil; s = s.prev {
		for i := range s.c {
			n += s.c[i].n.Load()
		}
	}
	return n != 0
}
