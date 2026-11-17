package slogx

import (
	"context"
	"hash/maphash"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestInflightCloseWaitsForLines(t *testing.T) {
	p := newPipeline()
	slot, ok := p.inflight.enter()
	if !ok {
		t.Fatal("enter on an open pipeline failed")
	}
	done := make(chan struct{})
	go func() { p.inflight.close(); close(done) }()
	select {
	case <-done:
		t.Fatal("close returned while a line was in flight")
	case <-time.After(20 * time.Millisecond):
	}
	p.inflight.leave(slot)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not return after the line left")
	}
	if _, ok := p.inflight.enter(); ok {
		t.Fatal("a line entered a closed pipeline")
	}
	if p.inflight.busy() {
		t.Fatal("a refused enter stayed counted")
	}
}

// TestRetiredOutputsSeeNoLines writes from many goroutines across
// Configure: no line may reach an output after it was closed.
func TestRetiredOutputsSeeNoLines(t *testing.T) {
	l := New()
	defer l.Close()
	stop := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		m := l.Module("x")
		for {
			select {
			case <-stop:
				return
			default:
				m.Info("m")
			}
		}
	}()
	for range 50 {
		o := &closeCheck{t: t}
		if err := l.Configure(Config{Console: ConsoleOff, Routes: []Route{{Output: o}}}); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	<-finished
}

type closeCheck struct {
	t      *testing.T
	closed bool // written by Close, read by Write: the race detector checks the hand-over
}

func (c *closeCheck) Write([]byte) error {
	if c.closed {
		c.t.Error("line written after Close")
	}
	return nil
}

func (c *closeCheck) Close() error { c.closed = true; return nil }

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestRotatingBuffer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.log")
	r, err := OpenRotating(Rotation{Path: path, BufferSize: 16, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Write([]byte("aaaa\n"))
	_ = r.Write([]byte("bbbb\n"))
	if got := readFile(t, path); got != "" {
		t.Fatalf("buffered lines written early: %q", got)
	}
	_ = r.Write([]byte("cccccccc\n")) // does not fit: the buffer goes first
	if got := readFile(t, path); got != "aaaa\nbbbb\n" {
		t.Fatalf("after overflow %q", got)
	}
	_ = r.Write([]byte("a line of sixteen+\n")) // past the buffer: straight through, after what is buffered
	if got := readFile(t, path); got != "aaaa\nbbbb\ncccccccc\na line of sixteen+\n" {
		t.Fatalf("after a long line %q", got)
	}
	_ = r.Write([]byte("dd\n"))
	if err := r.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); !strings.HasSuffix(got, "+\ndd\n") {
		t.Fatalf("after Flush %q", got)
	}
	_ = r.Write([]byte("ee\n"))
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); !strings.HasSuffix(got, "dd\nee\n") {
		t.Fatalf("Close lost the buffer: %q", got)
	}
}

func TestRotatingBufferGoesToOldFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.log")
	r, err := OpenRotating(Rotation{Path: path, MaxSize: 10, BufferSize: 64, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Write([]byte("first\n"))
	_ = r.Write([]byte("second\n")) // past MaxSize: rotates
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); got != "second\n" {
		t.Fatalf("current file %q", got)
	}
	files := r.rotatedFiles()
	old := make([]string, 0, len(files))
	for _, f := range files {
		old = append(old, readFile(t, f.path))
	}
	if len(old) != 1 || old[0] != "first\n" {
		t.Fatalf("rotated files %q", old)
	}
}

func TestRotatingBufferFlushInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "a.log")
		r, err := OpenRotating(Rotation{Path: path, BufferSize: 1024})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()
		_ = r.Write([]byte("x\n"))
		time.Sleep(defaultFlushInterval - time.Millisecond)
		synctest.Wait()
		if got := readFile(t, path); got != "" {
			t.Fatalf("flushed before the interval: %q", got)
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		if got := readFile(t, path); got != "x\n" {
			t.Fatalf("not flushed after the interval: %q", got)
		}
	})
}

func TestAsyncLinesAreCopied(t *testing.T) {
	o := &memOut{}
	a := Async(o, 16)
	line := []byte("one\n")
	_ = a.Write(line)
	copy(line, "two")
	_ = a.Write(line)
	_ = a.Close()
	if got := o.all(); got != "one\ntwo\n" {
		t.Fatalf("got %q", got)
	}
}

func TestAsyncWriteDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates")
	}
	a := Async(discard{}, 1024)
	defer func() { _ = a.Close() }()
	line := []byte("time=x level=INFO msg=m\n")
	for range 100 { // warm the pool
		_ = a.Write(line)
	}
	_ = a.Flush()
	if n := testing.AllocsPerRun(100, func() { _ = a.Write(line); _ = a.Flush() }); n > 1 {
		// Flush makes its channel; the line itself must not allocate
		t.Fatalf("%v allocations per line", n)
	}
}

type reqKey struct{}

func TestContextAppendDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates")
	}
	l := New()
	defer l.Close()
	if err := l.Configure(Config{Console: ConsoleOff, Routes: []Route{{Output: discard{}}}, ContextAppend: func(ctx context.Context, dst []slog.Attr) []slog.Attr {
		if v, ok := ctx.Value(reqKey{}).(string); ok {
			dst = append(dst, slog.String("req", v), slog.String("user", "u"))
		}
		return dst
	}}); err != nil {
		t.Fatal(err)
	}
	m := l.Module("x")
	ctx := context.WithValue(context.Background(), reqKey{}, "r1")
	if n := testing.AllocsPerRun(1000, func() { m.InfoContext(ctx, "done", "k", 1) }); n != 0 {
		t.Fatalf("%v allocations per line", n)
	}
}

func TestContextAppendAttrsAreResolved(t *testing.T) {
	l, o := withRoute(t, Config{ContextAppend: func(_ context.Context, dst []slog.Attr) []slog.Attr {
		return append(dst, slog.Group("g", slog.Int("a", 1)), slog.String(KeyModule, "forged"), slog.Any("s", Redacted("pw")))
	}}, nil)
	l.Module("x").InfoContext(context.Background(), "m")
	got := o.all()
	for _, want := range []string{"module=x ", "g.a=1", "_module=forged", "s=[REDACTED]"} {
		if !strings.Contains(got, want) {
			t.Fatalf("%q lacks %q", got, want)
		}
	}
}

func BenchmarkContextAppend(b *testing.B) {
	l := benchLogs(b, TextEncoder{})
	if err := l.Configure(Config{Console: ConsoleOff, Routes: []Route{{Encoder: TextEncoder{}, Output: discard{}}}, ContextAppend: func(ctx context.Context, dst []slog.Attr) []slog.Attr {
		if v, ok := ctx.Value(reqKey{}).(string); ok {
			dst = append(dst, slog.String("req", v))
		}
		return dst
	}}); err != nil {
		b.Fatal(err)
	}
	m := l.Module("x")
	ctx := context.WithValue(context.Background(), reqKey{}, "r1")
	b.ReportAllocs()
	for b.Loop() {
		m.InfoContext(ctx, "done", "k", 1)
	}
}

func TestKeyOfKeepsLinesApart(t *testing.T) {
	d := &Def{Name: "ev", Limit: "ip"}
	ev := func(attrs ...slog.Attr) sampleKey {
		return keyOf(&Record{Def: d, Event: d.Name, Attrs: attrs}, d.Limit)
	}
	plain := func(lv slog.Level, module, msg string) sampleKey {
		return keyOf(&Record{Level: lv, Module: module, Msg: msg}, "")
	}
	keys := []sampleKey{
		ev(),
		ev(slog.String("ip", "")),
		ev(slog.String("ip", "a")),
		plain(slog.LevelInfo, "m", "x"),
		plain(slog.LevelWarn, "m", "x"),
		plain(slog.LevelInfo, "ev", ""),
	}
	for i := range keys {
		for j := range i {
			if keys[i] == keys[j] {
				t.Fatalf("keys %d and %d are the same: %+v", j, i, keys[i])
			}
		}
	}
	if ev(slog.String("ip", "a")) != ev(slog.String("ip", "a"), slog.Int("n", 1)) {
		t.Fatal("an attribute other than the Limit field changed the key")
	}
}

func TestInflightGrowsAndCloseSeesEverySet(t *testing.T) {
	var f inflight
	var held []*atomic.Int64
	for gen := range 3 { // lines in the counter and in each stripe set
		for range 4 {
			c, _ := f.enter()
			held = append(held, c)
		}
		if gen < 2 {
			for range growAfter {
				f.collided()
			}
		}
	}
	if s := f.stripes.Load(); maxStripes() >= 4 && (s == nil || len(s.c) != 4 || s.prev == nil) {
		t.Fatal("no growth after collisions")
	}
	done := make(chan struct{})
	go func() { f.close(); close(done) }()
	// the base counter, then the newest set: the oldest set alone must
	// still hold close
	held = slices.Concat(held[:4], held[8:], held[4:8])
	for _, c := range held {
		select {
		case <-done:
			t.Fatal("close returned while lines were in flight")
		case <-time.After(5 * time.Millisecond):
		}
		f.leave(c)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not return")
	}
}

func TestInflightQuietStaysSingle(t *testing.T) {
	p := newPipeline()
	for range 1000 {
		c, _ := p.inflight.enter()
		p.inflight.leave(c)
	}
	if p.inflight.stripes.Load() != nil {
		t.Fatal("an uncontended pipeline grew stripes")
	}
}

func TestSamplerGrowsKeepingCounts(t *testing.T) {
	if maxStripes() < 2 {
		t.Skip("one P: never grows")
	}
	sm := newSamplerN(0)
	now := time.Now()
	s := Sampling{First: 1, Tick: time.Minute}
	for i := range 16 {
		sm.allow(sampleKey{a: itoa(i)}, s, now)
	}
	sm.grow(sm.set.Load())
	set := sm.set.Load()
	if len(set.shards) != 2 {
		t.Fatalf("%d shards after growth", len(set.shards))
	}
	for i := range 16 {
		if _, ok := sm.allow(sampleKey{a: itoa(i)}, s, now); ok {
			t.Fatalf("key %d lost its count in the move", i)
		}
	}
	hot := newSamplerN(0)
	hot.allow(sampleKey{a: "one"}, s, now)
	hot.grow(hot.set.Load())
	if len(hot.set.Load().shards) != 1 {
		t.Fatal("a sampler of one key split")
	}
}

func TestSamplerGrowsUnderContention(t *testing.T) {
	if maxStripes() < 2 {
		t.Skip("one P: never grows")
	}
	sm := newSamplerN(0)
	now := time.Now()
	s := Sampling{First: 1, Tick: time.Minute}
	for i := range 64 {
		sm.allow(sampleKey{a: itoa(i)}, s, now)
	}
	set := sm.set.Load()
	sh := &set.shards[0]
	sh.mu.Lock()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		sm.allow(sampleKey{a: "late"}, s, now) // waits on the held lock while the set moves
	}()
	deadline := time.Now().Add(5 * time.Second)
	for sm.collisions.Load() == 0 { // the line found the lock held
		if time.Now().After(deadline) {
			sh.mu.Unlock()
			t.Fatal("the line never waited")
		}
		time.Sleep(time.Millisecond)
	}
	sm.growLocked(set)
	sh.mu.Unlock()
	<-finished
	sm.collisions.Store(growAfter - 1) // the next wait makes it grow by itself
	set = sm.set.Load()
	key := sampleKey{a: "0"}
	sh = &set.shards[maphash.Comparable(sm.seed, key)&set.mask] // the shard the line will wait on
	sh.mu.Lock()
	grown := make(chan struct{})
	go func() {
		defer close(grown)
		sm.allow(key, s, now)
	}()
	for sm.collisions.Load() != 0 { // reset once the line decided to grow
		if time.Now().After(deadline) {
			sh.mu.Unlock()
			t.Fatal("a line waiting on the lock never decided to grow")
		}
		time.Sleep(time.Millisecond)
	}
	sh.mu.Unlock()
	<-grown
	if maxStripes() >= 4 && len(sm.set.Load().shards) != 4 {
		t.Fatalf("%d shards after contention", len(sm.set.Load().shards))
	}

	if _, ok := sm.allow(sampleKey{a: "late"}, s, now); ok {
		t.Fatal("the line that waited out the growth counted in the old shards")
	}
}

func TestRotatingTimedFlushErrorIsReturned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, err := OpenRotating(Rotation{Path: filepath.Join(t.TempDir(), "a.log"), BufferSize: 1024})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()
		_ = r.Write([]byte("x\n"))
		r.mu.Lock()
		_ = r.f.Close() // the timed flush will fail
		r.mu.Unlock()
		time.Sleep(defaultFlushInterval)
		synctest.Wait()
		if err := r.Write([]byte("y\n")); err == nil {
			t.Fatal("the timed flush's error was lost")
		}
		if err := r.Write([]byte("z\n")); err != nil {
			t.Fatalf("the error was returned twice: %v", err)
		}
	})
}

// TestSamplerGrowthMergesOverflow fills two shards past their share, so
// each holds an overflow slot, then grows: the suppressed counts of both
// must survive in the one overflow slot of the new generation.
func TestSamplerGrowthMergesOverflow(t *testing.T) {
	sm := newSamplerN(1)
	now := time.Now()
	s := Sampling{First: 1}
	for i := range 20000 {
		sm.allow(sampleKey{a: itoa(i)}, s, now)
	}
	overflow := func(set *sampleSet) (slots int, suppressed uint64) {
		for i := range set.shards {
			if sl, ok := set.shards[i].seen[sampleKey{overflow: true}]; ok {
				slots++
				suppressed += sl.suppressed
			}
		}
		return slots, suppressed
	}
	set := sm.set.Load()
	slots, before := overflow(set)
	if slots != 2 || before == 0 {
		t.Fatalf("setup: %d overflow slots, %d suppressed", slots, before)
	}
	for i := range set.shards {
		set.shards[i].mu.Lock()
	}
	sm.growLocked(set)
	for i := range set.shards {
		set.shards[i].mu.Unlock()
	}
	if slots, after := overflow(sm.set.Load()); slots != 1 || after != before {
		t.Fatalf("after growth %d overflow slots, %d suppressed, want 1 and %d", slots, after, before)
	}
}
