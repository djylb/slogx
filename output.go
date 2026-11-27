package slogx

import (
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// Output receives encoded lines, one per Write. Write must be safe for
// concurrent use, and must not keep line after it returns: the buffer is
// reused.
type Output interface {
	Write(line []byte) error
	Close() error
}

// Flusher is an Output that holds lines back; Flush writes them out.
type Flusher interface {
	Flush() error
}

// Reopener is an Output backed by a file that an external rotation may
// move away; Reopen opens it again.
type Reopener interface {
	Reopen() error
}

// WriterOutput writes lines to w under a lock; Close closes w only if it is
// an io.Closer other than os.Stdout or os.Stderr.
func WriterOutput(w io.Writer) Output { return &writerOutput{w: w} }

type writerOutput struct {
	mu sync.Mutex
	w  io.Writer
}

func (o *writerOutput) Write(p []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	n, err := o.w.Write(p)
	if err == nil && n != len(p) {
		return io.ErrShortWrite
	}
	return err
}

func (o *writerOutput) Close() error {
	if o.w == os.Stdout || o.w == os.Stderr {
		return nil
	}
	if c, ok := o.w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Async returns an Output that hands lines to o on a goroutine of its own
// through a queue of size lines. A line that does not fit is dropped and
// counted rather than blocking the caller. Flush waits for earlier writes
// and flushes o. Close drains and closes o. Both wait for o, so if o stalls, they
// stall too.
func Async(o Output, size int) *AsyncOutput {
	return newAsync(o, size, true)
}

func newAsync(o Output, size int, owns bool) *AsyncOutput {
	a := &AsyncOutput{out: o, q: make(chan asyncItem, max(size, 1)), done: make(chan struct{})}
	a.owns = owns
	return a
}

// AsyncOutput is the Output of Async.
type AsyncOutput struct {
	// OnDrop, when set before the first Write, is called from the queue's
	// goroutine once the queue has drained after dropping lines, with how
	// many were dropped since the last call. It may log: the queue has room.
	OnDrop func(dropped uint64)

	slots    sync.Once
	running  atomic.Bool
	free     chan *lineSlot
	pending  error // accessed only by the active worker
	total    *atomic.Uint64
	failures *atomic.Uint64
	out      Output
	q        chan asyncItem
	done     chan struct{}
	dropped  atomic.Uint64
	reported uint64 // run goroutine only
	closeMu  sync.RWMutex
	barriers sync.WaitGroup
	result   error
	idleExit bool // fallback queues outlive Logs.Close
	owns     bool
	closed   bool
}

type asyncItem struct {
	line   *lineSlot
	reply  chan error
	reopen bool
	final  bool
}

// Each queued line owns a slot until the worker finishes writing it. Short
// lines fit inline even when a burst fills a previously idle queue.
type lineSlot struct {
	line   []byte
	inline [256]byte
}

func (a *AsyncOutput) initSlots() {
	slots := make([]lineSlot, cap(a.q)+1)
	a.free = make(chan *lineSlot, len(slots))
	for i := range slots {
		slots[i].line = slots[i].inline[:0]
		a.free <- &slots[i]
	}
}

func (a *AsyncOutput) run() {
	for {
		var it asyncItem
		select {
		case it = <-a.q:
		default:
			if !a.idleExit {
				it = <-a.q
				break
			}
			// Hand ownership back to producers. Recheck for an enqueue
			// whose producer still saw this worker running.
			a.running.Store(false)
			if len(a.q) > 0 && a.running.CompareAndSwap(false, true) {
				continue
			}
			return
		}
		if it.final {
			a.reportDrops()
			a.result = errors.Join(a.pending, flushOutput(a.out))
			if a.owns {
				a.result = errors.Join(a.result, a.out.Close())
			}
			close(a.done)
			return
		}
		if it.reply != nil {
			err := flushOutput(a.out)
			if it.reopen {
				err = errors.Join(err, reopenOutput(a.out))
			}
			it.reply <- errors.Join(a.pending, err)
			a.pending = nil
		} else {
			if err := a.out.Write(it.line.line); err != nil {
				if a.failures != nil {
					a.failures.Add(1)
				}
				if a.pending == nil {
					a.pending = err
				}
			}
			if cap(it.line.line) > 64<<10 {
				it.line.line = it.line.inline[:0]
			}
			a.free <- it.line
		}
		if len(a.q) == 0 {
			a.reportDrops()
		}
	}
}

func (a *AsyncOutput) reportDrops() {
	if n := a.dropped.Load(); n > a.reported {
		if a.OnDrop != nil {
			a.OnDrop(n - a.reported)
		}
		a.reported = n
	}
}

func (a *AsyncOutput) startWorker() {
	if a.running.CompareAndSwap(false, true) {
		go a.run()
	}
}
func (a *AsyncOutput) drop() {
	a.dropped.Add(1)
	if a.total != nil {
		a.total.Add(1)
	}
}

// Write queues a copy of p, in a buffer reused once the line is written,
// or drops it when the queue is full.
func (a *AsyncOutput) Write(p []byte) error {
	a.closeMu.RLock()
	defer a.closeMu.RUnlock()
	if a.q == nil || a.closed {
		return os.ErrClosed
	}
	if len(a.q) == cap(a.q) { // full: drop without copying
		a.drop()
		return nil
	}
	a.slots.Do(a.initSlots)
	select {
	case slot := <-a.free:
		slot.line = append(slot.line[:0], p...)
		select {
		case a.q <- asyncItem{line: slot}:
			a.startWorker()
		default:
			a.free <- slot
			a.drop()
		}
	default:
		a.drop()
	}
	return nil
}

// Dropped reports how many lines did not fit in the queue.
func (a *AsyncOutput) Dropped() uint64 { return a.dropped.Load() }

// Flush waits until the lines queued before it are written and o flushed.
func (a *AsyncOutput) Flush() error {
	return a.barrier(false)
}

func flushOutput(o Output) error {
	if f, ok := o.(Flusher); ok {
		return f.Flush()
	}
	return nil
}
func reopenOutput(o Output) error {
	if f, ok := o.(Reopener); ok {
		return f.Reopen()
	}
	return nil
}

// Reopen flushes earlier lines before reopening the underlying output.
func (a *AsyncOutput) Reopen() error { return a.barrier(true) }

func (a *AsyncOutput) barrier(reopen bool) error {
	a.closeMu.RLock()
	if a.q == nil || a.closed {
		a.closeMu.RUnlock()
		return os.ErrClosed
	}
	a.barriers.Add(1)
	a.closeMu.RUnlock()
	defer a.barriers.Done()
	reply := make(chan error)
	a.q <- asyncItem{reply: reply, reopen: reopen}
	a.startWorker()
	return <-reply
}

// Close drains the queue and returns the final flush and close result.
func (a *AsyncOutput) Close() error {
	a.closeMu.Lock()
	if a.q == nil {
		a.closeMu.Unlock()
		return nil
	}
	first := !a.closed
	a.closed = true
	a.closeMu.Unlock()
	if first {
		a.barriers.Wait()
		a.q <- asyncItem{final: true}
		a.startWorker()
	}
	<-a.done
	return a.result
}
