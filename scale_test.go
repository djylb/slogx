package slogx

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
