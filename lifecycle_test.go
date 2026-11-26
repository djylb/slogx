package slogx

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestReconfigureDuringRetirement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New()
		out := &closeCheck{entered: make(chan struct{}), release: make(chan struct{})}
		cfg := Config{Console: ConsoleOff, LogSecrets: true, Routes: []Route{{Output: out}}}
		check(t, l.Configure(cfg))
		m := l.Module("m")
		m.Info("hold")
		<-out.entered
		done := make(chan error, 1)
		go func() { done <- l.Configure(Config{Console: ConsoleOff, Memory: 4}) }()
		synctest.Wait()
		m.Info("new", "secret", Redacted("plaintext"))
		if got := l.Memory().Text(Query{}, TextEncoder{}); !strings.Contains(got, "secret=[REDACTED]") {
			t.Fatalf("new destination: %s", got)
		}
		close(out.release)
		check(t, <-done)
		// A closing generation cannot stop an override in its successor.
		out = &closeCheck{entered: make(chan struct{}), release: make(chan struct{})}
		cfg.Routes[0].Output = out
		cfg.LogSecrets = false
		check(t, l.Configure(cfg))
		m.Info("hold")
		<-out.entered
		go func() { done <- l.Close() }()
		synctest.Wait()
		configured := make(chan error, 1)
		go func() { configured <- l.Configure(Config{Console: ConsoleOff}) }()
		synctest.Wait()
		if len(configured) != 0 {
			t.Error("Configure returned during Close")
		}
		check(t, l.Levels().Override("m", slog.LevelDebug, time.Hour))
		close(out.release)
		check(t, <-done)
		check(t, <-configured)
		if got := l.Levels().Of("m"); got != slog.LevelDebug {
			t.Fatal(got)
		}
		check(t, l.Close())
	})
}

func TestDetachDuringDispatch(t *testing.T) {
	l := New()
	check(t, l.Configure(Config{Console: ConsoleOff}))
	entered, release := make(chan struct{}), make(chan struct{})
	first, err := l.Attach(Route{Func: func(*Record) { close(entered); <-release }})
	check(t, err)
	out := &memOut{}
	detach, err := l.Attach(Route{Output: out})
	check(t, err)
	done := make(chan struct{})
	go func() { l.Module("m").Info("in flight"); close(done) }()
	<-entered
	check(t, detach())
	check(t, out.Close())
	close(release)
	<-done
	check(t, first())
	check(t, l.Close())
	if got := out.all(); got != "" {
		t.Fatal(got)
	}
}

func TestSharedFileHandover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.gz")
	rot := Rotation{Path: path, MaxSize: 90, Compress: true}
	cfg := Config{Console: ConsoleOff, File: rot, SecurityFile: rot}
	l := New()
	check(t, l.Configure(cfg))
	t.Cleanup(func() { check(t, l.Close()) })
	m := l.Module("m")
	d := Def{Name: "security", Security: true}
	for i := range 48 {
		if i%8 == 0 {
			check(t, l.Configure(cfg))
		}
		Emit(t.Context(), m, &d, "seq", i)
	}
	check(t, l.Close())
	paths, err := filepath.Glob(filepath.Join(filepath.Dir(path), archiveRoot, filepath.Base(path), "*"))
	check(t, err)
	seen := map[string]int{}
	for _, p := range append(paths, path) {
		data, readErr := os.ReadFile(p)
		check(t, readErr)
		if p != path {
			z, gzErr := gzip.NewReader(bytes.NewReader(data))
			check(t, gzErr)
			data, readErr = io.ReadAll(z)
			check(t, readErr)
			check(t, z.Close())
		}
		for line := range strings.FieldsSeq(string(data)) {
			if strings.HasPrefix(line, "seq=") {
				seen[line]++
			}
		}
	}
	for i := range 48 {
		if seen[fmt.Sprintf("seq=%d", i)] != 1 {
			t.Fatalf("record %d: %v", i, seen)
		}
	}
}

func TestStatusAndSecurityFallback(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "stderr")
	check(t, err)
	previous := os.Stderr
	os.Stderr = f
	defer func() { os.Stderr = previous; _ = f.Close() }()
	for _, level := range []string{"off", "error"} {
		l := New()
		check(t, l.Configure(Config{Console: ConsoleOff, Memory: 4, Attrs: []slog.Attr{slog.String("instance", "test")}, Level: level, SecurityFile: Rotation{Path: filepath.Join(f.Name(), "security")}, LogSecrets: true}))
		for _, name := range []string{EventLogFallback, EventLogSecrets} {
			entries := l.Memory().Query(Query{Events: []string{name}})
			if len(entries) != 1 || !slices.Contains(entries[0].Attrs, Field{Key: "instance", Value: "test"}) {
				t.Fatal(entries)
			}
		}
		Emit(t.Context(), l.Module("m"), &Def{Name: "audit", Security: true, Level: slog.LevelDebug})
		check(t, l.Close())
	}
	data, err := os.ReadFile(f.Name())
	check(t, err)
	if n := strings.Count(string(data), " instance=test"); n != 6 {
		t.Fatal(string(data))
	}
	for _, name := range []string{"log_secrets", "log_fallback", "audit"} {
		if strings.Count(string(data), "event="+name+" ") != 2 {
			t.Fatalf("%s: %s", name, data)
		}
	}
}

func TestMergedStderrFallback(t *testing.T) {
	for _, console := range []Console{ConsoleStderr, ConsoleOff, ConsoleStdout} {
		f, err := os.CreateTemp(t.TempDir(), "stderr")
		check(t, err)
		func() {
			previous := os.Stderr
			os.Stderr = f
			defer func() { os.Stderr = previous; check(t, f.Close()) }()
			bad := Rotation{Path: filepath.Join(f.Name(), "file")}
			l := New()
			defer func() { check(t, l.Close()) }()
			check(t, l.Configure(Config{Console: console, Level: "warn", File: bad, SecurityFile: bad}))
			m := l.Module("m")
			for _, d := range []Def{{Name: "above", Security: true, Level: slog.LevelWarn}, {Name: "below", Security: true, Level: slog.LevelDebug}} {
				Emit(t.Context(), m, &d)
			}
			m.Warn("normal")
			m.Debug("filtered")
		}()
		lines := readFile(t, f.Name())
		for _, marker := range []string{"event=above ", "event=below ", "msg=normal\n"} {
			if strings.Count(lines, marker) != 1 || strings.Contains(lines, "filtered") {
				t.Fatalf("console %d, %q: %s", console, marker, lines)
			}
		}
	}
}

func TestMemoryOwnershipAndRetirement(t *testing.T) {
	m := NewMemory(10000)
	modules := []string{"a"}
	sub := m.Subscribe(t.Context(), Query{Modules: modules}, 1)
	modules[0] = "b"
	r := Record{Module: "a", Source: "caller", Stack: "stack", Attrs: []slog.Attr{slog.String("v", "original")}}
	m.add(&r)
	e := <-sub.C()
	e.Attrs[0].Value = "subscriber"
	got := m.Query(Query{Limit: 1})
	got[0].Attrs[0].Value = "query"
	taken := m.Take()
	taken[0].Attrs[0].Value = "take"
	if line := m.Text(Query{}, TextEncoder{}); line != string(TextEncoder{}.Encode(nil, &r)) {
		t.Fatalf("export %q", line)
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 200 {
			m.add(&r)
		}
	})
	for range 200 {
		if m.Size() != 10000 {
			t.Fatal("capacity")
		}
	}
	wg.Wait()
	if es := m.Query(Query{Limit: 1}); len(es) != 1 || es[0].Seq != 201 || cap(es) != 1 {
		t.Fatalf("limit: len=%d cap=%d", len(es), cap(es))
	}
	m.closeSubscriptions()
	for range sub.C() {
	}
	if _, ok := <-m.Subscribe(t.Context(), Query{}, 1).C(); ok {
		t.Fatal("late subscription")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, ok := <-NewMemory(1).Subscribe(ctx, Query{}, 1).C(); ok {
		t.Fatal("canceled subscription")
	}
}

func TestGroupedSamplingAndRedaction(t *testing.T) {
	l := newBuffered(t, "info")
	m := l.Module("x").With("module", "pretend").WithGroup("g").With("event", "bound")
	d := Def{Name: "limited", Limit: "id"}
	for _, id := range []string{"a", "b", "a"} {
		Emit(t.Context(), m, &d, "id", id, "event", "inline")
	}
	got := takeText(l)
	if strings.Count(got, "g.id=") != 2 || !strings.Contains(got, "module=x") || !strings.Contains(got, "_module=pretend") || !strings.Contains(got, "g.event=inline") {
		t.Fatal(got)
	}
	val := struct {
		Field   Secret
		private Secret
	}{Redacted("sensitive"), Redacted("private-value")}
	m.Info(fmt.Sprint(val), "v", []any{val})
	for _, enc := range []Encoder{TextEncoder{}, JSONEncoder{}} {
		line := l.Memory().Text(Query{}, enc)
		if strings.Contains(line, "sensitive") || strings.Contains(line, "private-value") {
			t.Fatalf("leak: %s", line)
		}
	}
}

type faultyOutput struct {
	ops                          []string
	writeErr, flushErr, closeErr error
}

func (o *faultyOutput) Write([]byte) error { o.ops = append(o.ops, "write"); return o.writeErr }
func (o *faultyOutput) Flush() error       { o.ops = append(o.ops, "flush"); return o.flushErr }
func (o *faultyOutput) Reopen() error      { o.ops = append(o.ops, "reopen"); return nil }
func (o *faultyOutput) Close() error       { o.ops = append(o.ops, "close"); return o.closeErr }

func TestAsyncBarriers(t *testing.T) {
	o := &faultyOutput{writeErr: errors.New("write failed"), flushErr: errors.New("flush failed"), closeErr: errors.New("close failed")}
	a := Async(o, 2)
	check(t, a.Write([]byte("line")))
	err := a.Reopen()
	if !errors.Is(err, o.writeErr) || !errors.Is(err, o.flushErr) {
		t.Fatalf("reopen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	for _, e := range []error{a.Close(), <-done} {
		if !errors.Is(e, o.flushErr) || !errors.Is(e, o.closeErr) {
			t.Fatalf("close: %v", e)
		}
	}
	if !slices.Equal(o.ops, []string{"write", "flush", "reopen", "flush", "close"}) {
		t.Fatal(o.ops)
	}
}

func TestFullQueueDuringFlushAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		block := make(chan struct{})
		a := Async(&blockingOut{block: block}, 1)
		check(t, a.Write(nil))
		synctest.Wait()
		check(t, a.Write(nil))
		flushed, closed := make(chan error, 1), make(chan error, 1)
		go func() { flushed <- a.Flush() }()
		synctest.Wait()
		go func() { closed <- a.Close() }()
		synctest.Wait()
		if err := a.Write(nil); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("write during close: %v", err)
		}
		close(block)
		check(t, <-flushed)
		check(t, <-closed)
	})
}

func TestDefaultCapture(t *testing.T) {
	w, flags, prefix := log.Writer(), log.Flags(), log.Prefix()
	defer func() { log.SetOutput(w); log.SetFlags(flags); log.SetPrefix(prefix) }()
	for _, l := range []*slog.Logger{slog.Default(), slog.Default().With("k", 1).WithGroup("g")} {
		if CaptureStdLog(l, slog.LevelInfo) == nil {
			t.Fatal("accepted recursive handler")
		}
		if log.Writer() != w || log.Flags() != flags || log.Prefix() != prefix {
			t.Fatal("rejected capture changed the standard logger")
		}
	}
}

func TestStrictLevelChanges(t *testing.T) {
	l := newBuffered(t, "off")
	if l.Module("m").Enabled(t.Context(), LevelOff+1) {
		t.Fatal("off enabled")
	}
	for _, module := range []string{"", "m"} {
		check(t, l.Levels().Override(module, LevelOff+1, time.Hour))
		if !l.Module("m").Enabled(t.Context(), LevelOff+1) || l.Module("m").Enabled(t.Context(), slog.LevelError) {
			t.Fatal("custom override of off", l.Levels().Of("m"))
		}
		l.Levels().Clear(module)
	}
	for _, body := range []string{`{"level":"debug"}{}`, `{"level":"debug","ttl":"1m"}`, `{"level":"db="}`} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		l.Levels().ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest || l.Levels().String() != "off" {
			t.Fatal(w.Code, l.Levels().String())
		}
	}
}

func TestMarshaledControls(t *testing.T) {
	val := "before\u2028\u2029\u202e\u0085after"
	r := Record{Stack: val, Attrs: []slog.Attr{slog.Any("v", json.RawMessage(`"`+val+`"`))}}
	for _, enc := range []Encoder{TextEncoder{}, JSONEncoder{}, PrettyEncoder{}} {
		b := enc.Encode(nil, &r)
		if bytes.ContainsAny(b, "\u2028\u2029\u202e\u0085") {
			t.Fatalf("%T: %q", enc, b)
		}
	}
}

func TestOverflowPolicies(t *testing.T) {
	sm := newSamplerN(0)
	now := time.Now()
	burst := Sampling{First: 1, Tick: time.Minute}
	for i := range maxSampleKeys {
		sm.allow(sampleKey{a: itoa(i)}, burst, now)
	}
	key := sampleKey{a: "untracked"}
	sm.allow(key, burst, now)
	sm.allow(key, burst, now)
	other := Sampling{First: 2, Tick: time.Hour}
	for range 2 {
		if n, ok := sm.allow(key, other, now); !ok || n != 0 {
			t.Fatalf("cross-policy overflow: %d %v", n, ok)
		}
	}
	sm.allow(sampleKey{a: "later"}, burst, now.Add(2*time.Minute))
	if sm.unattributed.Load() != 1 {
		t.Fatal("unattributed count", sm.unattributed.Load())
	}
}

func TestBelowRouteIndependence(t *testing.T) {
	l := newBuffered(t, "off")
	var counts [2]int
	for i := range counts {
		_, err := l.Attach(Route{Below: i == 1, Match: MatchMinLevel(slog.LevelDebug), Func: func(*Record) { counts[i]++ }})
		check(t, err)
	}
	l.Module("m").Debug("one")
	check(t, l.Close())
	if counts != [2]int{0, 1} {
		t.Fatal(counts)
	}
}

func TestInterruptedCompression(t *testing.T) {
	dir := t.TempDir()
	owner := filepath.Join(dir, archiveRoot, "a.log")
	check(t, os.MkdirAll(owner, 0o700))
	archive := filepath.Join(owner, "20261108T000000.000")
	check(t, os.WriteFile(archive, []byte("original"), 0o600))
	check(t, os.WriteFile(archive+".gz.tmp", []byte("partial"), 0o600))
	r, err := OpenRotating(Rotation{Path: filepath.Join(dir, "a.log"), Compress: true})
	check(t, err)
	check(t, r.Close())
	b, err := os.ReadFile(archive + ".gz")
	check(t, err)
	z, err := gzip.NewReader(bytes.NewReader(b))
	check(t, err)
	b, err = io.ReadAll(z)
	check(t, err)
	check(t, z.Close())
	if string(b) != "original" {
		t.Fatal("incomplete archive")
	}
	if _, err = os.Stat(archive + ".gz.tmp"); !os.IsNotExist(err) {
		t.Fatal("temporary file survived", err)
	}
}

func TestArchiveOwnership(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "audit-20260101T000000.000.gz")
	check(t, os.WriteFile(legacy, []byte("ambiguous"), 0o600))
	a, err := OpenRotating(Rotation{Path: filepath.Join(dir, "audit.gz"), MaxSize: 1})
	check(t, err)
	check(t, a.Write([]byte("first")))
	check(t, a.Write([]byte("second")))
	owner := a.file.Load()
	check(t, a.Close())
	files, err := owner.scan()
	check(t, err)
	if len(files) != 1 {
		t.Fatalf("archives: %v", files)
	}
	b, err := OpenRotating(Rotation{Path: filepath.Join(dir, "audit"), Compress: true, MaxAge: time.Nanosecond})
	check(t, err)
	check(t, b.Close())
	if readFile(t, files[0].path) != "first" || readFile(t, legacy) != "ambiguous" {
		t.Fatal("sibling or legacy archive changed")
	}
	if _, err = OpenRotating(Rotation{Path: files[0].path}); !errors.Is(err, errRotationConflict) {
		t.Fatalf("archive opened as active log: %v", err)
	}
}

func TestExternalRotationPreservesReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live")
	r, err := OpenRotating(Rotation{Path: path, MaxSize: 1})
	check(t, err)
	defer func() { check(t, r.Close()) }()
	check(t, r.Write([]byte("old")))
	alias := filepath.Join(dir, "alias")
	if err = os.Link(path, alias); err == nil {
		if other, e := OpenRotating(Rotation{Path: alias}); e == nil {
			check(t, other.Close())
			t.Fatal("opened hard-link alias")
		} else if !errors.Is(e, errRotationConflict) {
			t.Fatal(e)
		}
	}
	moved := filepath.Join(dir, "moved")
	if err = os.Rename(path, moved); err != nil && runtime.GOOS == "windows" {
		t.Skipf("renaming an open file unavailable: %v", err)
	}
	check(t, err)
	check(t, os.WriteFile(path, []byte("replacement"), 0o600))
	check(t, r.Write([]byte(" pending")))
	if readFile(t, path) != "replacement" || readFile(t, moved) != "old pending" {
		t.Fatal("rotated another writer's replacement")
	}
	if other, e := OpenRotating(Rotation{Path: moved}); e == nil {
		check(t, other.Close())
		t.Fatal("lost ownership of renamed open file")
	} else if !errors.Is(e, errRotationConflict) {
		t.Fatal(e)
	}
	check(t, r.Reopen())
	check(t, r.Write([]byte("new")))
	if readFile(t, path) != "new" || readFile(t, moved) != "old pending" {
		t.Fatal("Reopen did not switch to the replacement")
	}
}

func TestStaleFlushAndRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "buffer.log")
		r, err := OpenRotating(Rotation{Path: path, BufferSize: 64})
		check(t, err)
		file := r.file.Load()
		check(t, r.Write([]byte("first")))
		time.Sleep(defaultFlushInterval / 2)
		check(t, r.Flush())
		check(t, r.Write([]byte("second")))
		time.Sleep(defaultFlushInterval / 2)
		file.flushOnTime() // callback for the first batch finally acquires the lock
		if readFile(t, path) != "first" {
			t.Fatal("stale callback flushed the new batch")
		}
		file.mu.Lock()
		check(t, file.f.Close())
		file.mu.Unlock()
		time.Sleep(defaultFlushInterval)
		synctest.Wait()
		file.mu.Lock()
		pending, size := string(file.buf), file.size
		check(t, file.open())
		file.mu.Unlock()
		if pending != "second" || size != 5 {
			t.Fatalf("pending=%q committed=%d", pending, size)
		}
		if r.Write([]byte("third")) == nil {
			t.Fatal("timed failure was not reported")
		}
		time.Sleep(defaultFlushInterval)
		synctest.Wait()
		if readFile(t, path) != "firstsecondthird" {
			t.Fatal("timer did not retry suffix")
		}
		check(t, r.Close())
		if readFile(t, path) != "firstsecondthird" {
			t.Fatal("retry lost bytes")
		}
	})
}

func TestPlainHandlerAndHTTPFiltering(t *testing.T) {
	var output bytes.Buffer
	l := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelWarn}))
	Emit(t.Context(), l, &Def{Name: "quiet", Security: true, Level: slog.LevelDebug})
	HTTPErrorLog(l, "server").Print("message quotes http: TLS handshake error from somewhere")
	if got := output.String(); strings.Contains(got, "quiet") || !strings.Contains(got, "level=WARN") {
		t.Fatal(got)
	}
}

func TestPublishedSampling(t *testing.T) {
	policy := Sampling{First: 1}
	l := New()
	check(t, l.Configure(Config{Console: ConsoleOff, Memory: 4, Sample: &policy}))
	policy.First = 2
	for range 2 {
		l.Module("m").Info("one")
	}
	if got := len(l.Memory().Take()); got != 1 {
		t.Fatal(got)
	}
	check(t, l.Close())
	defer func() {
		if recover() == nil {
			t.Error("accepted invalid unregistered event")
		}
	}()
	Emit(t.Context(), l.Module("m"), &Def{Sample: &Sampling{First: -1}})
}

func TestFlushDuringRetirement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New()
		out := &closeCheck{entered: make(chan struct{}), release: make(chan struct{})}
		check(t, l.Configure(Config{Console: ConsoleOff, Routes: []Route{{Output: out}}}))
		l.Module("m").Info("already submitted")
		<-out.entered
		done := make(chan error, 1)
		go func() { done <- l.Configure(Config{Console: ConsoleOff}) }()
		synctest.Wait()
		flushed := make(chan error, 1)
		go func() { flushed <- l.Flush() }()
		synctest.Wait()
		if len(flushed) > 0 {
			t.Error("Flush skipped retired queue")
		}
		close(out.release)
		check(t, <-done)
		check(t, <-flushed)
		check(t, l.Close())
	})
}

func TestRejectedRotationPreservesArchives(t *testing.T) {
	dir := t.TempDir()
	l := New()
	cfg := Config{Console: ConsoleOff, File: Rotation{Path: filepath.Join(dir, "live.log")}}
	check(t, l.Configure(cfg))
	t.Cleanup(func() { check(t, l.Close()) })
	owner := filepath.Join(dir, archiveRoot, "new.log")
	check(t, os.MkdirAll(owner, 0o700))
	names := []string{"20260101T000000.000", "20260102T000000.000"}
	for _, n := range names {
		check(t, os.WriteFile(filepath.Join(owner, n), []byte(n), 0600))
	}
	conflict := cfg.File
	conflict.MaxFiles = 2
	if l.Configure(Config{Console: ConsoleOff, File: Rotation{Path: filepath.Join(dir, "new.log"), MaxFiles: 1}, SecurityFile: conflict}) == nil {
		t.Fatal("accepted conflict")
	}
	for _, n := range names {
		if got := readFile(t, filepath.Join(owner, n)); got != n {
			t.Error(got)
		}
	}
	l.Module("m").Info("still active")
	check(t, l.Flush())
	if !strings.Contains(readFile(t, cfg.File.Path), "still active") {
		t.Fatal("old configuration lost")
	}
}

func TestGroupedLimitRoot(t *testing.T) {
	l := newBuffered(t, "info")
	d := Def{Name: "limited", Limit: "id"}
	for _, id := range []string{"a", "b", "a"} {
		Emit(t.Context(), l.Module("m").With("id", id).WithGroup("g"), &d)
	}
	if got := len(l.Memory().Take()); got != 2 {
		t.Fatal(got)
	}
}

func TestClosedFallbackDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New()
		check(t, l.Close())
		l.Module("m").Info("after close")
		check(t, l.Flush())
	})
}

func TestRotationZeroAndDefaults(t *testing.T) {
	var zero Rotating
	for _, op := range []func() error{func() error { return zero.Write(nil) }, zero.Flush, zero.Reopen} {
		if err := op(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("zero handle: %v", err)
		}
	}
	check(t, zero.Close())
	cfg := Rotation{Path: filepath.Join(t.TempDir(), "buffer.log"), BufferSize: 64}
	a, err := OpenRotating(cfg)
	check(t, err)
	t.Cleanup(func() { check(t, a.Close()) })
	cfg.Mode = 0640
	cfg.FlushInterval = time.Second
	b, err := OpenRotating(cfg)
	check(t, err)
	check(t, b.Close())
}

func TestRotationBelowSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	check(t, os.Mkdir(target, 0o700))
	link := filepath.Join(dir, "link")
	makeLink := func(name string) {
		t.Helper()
		if err := os.Symlink(filepath.Join(dir, name), link); err != nil {
			if runtime.GOOS != "windows" {
				t.Skipf("directory symlink unavailable: %v", err)
			}
			// A directory junction exercises reparse-point resolution on
			// Windows accounts without the symbolic-link privilege.
			cmd := exec.CommandContext(t.Context(), "cmd.exe", "/d", "/c", "mklink", "/j", "link", name)
			cmd.Dir = dir
			if out, junctionErr := cmd.CombinedOutput(); junctionErr != nil {
				t.Skipf("directory links unavailable: %v: %s", junctionErr, out)
			}
		}
	}
	makeLink("target")
	cfg := Rotation{Path: filepath.Join(link, "one", "two", "events.log")}
	a, err := OpenRotating(cfg)
	check(t, err)
	t.Cleanup(func() { check(t, a.Close()) })
	for _, path := range []string{cfg.Path, filepath.Join(target, "one", "two", "events.log")} {
		b, openErr := OpenRotating(Rotation{Path: path})
		check(t, openErr)
		if a.file.Load() != b.file.Load() {
			t.Error("directory alias opened a second writer")
		}
		check(t, b.Close())
	}
	old, err := OpenRotating(Rotation{Path: filepath.Join(target, "one", "two", "events.log")})
	check(t, err)
	defer func() { check(t, old.Close()) }()
	l := New()
	defer func() { check(t, l.Close()) }()
	check(t, l.Configure(Config{Console: ConsoleOff, File: Rotation{Path: old.path}, SecurityFile: cfg}))
	m := l.Module("audit")
	d := Def{Name: "same", Security: true}
	Emit(t.Context(), m, &d)
	check(t, l.Flush())
	if n := strings.Count(readFile(t, old.path), "event=same "); n != 1 {
		t.Fatal("shared destination received", n)
	}
	check(t, os.Remove(link))
	newDir := filepath.Join(dir, "next")
	check(t, os.Mkdir(newDir, 0o700))
	makeLink("next")
	check(t, a.Reopen())
	check(t, a.Write([]byte("new")))
	check(t, old.Write([]byte("old")))
	if readFile(t, cfg.Path) != "new" || !strings.HasSuffix(readFile(t, old.path), "old") {
		t.Fatal("Reopen changed the wrong handle")
	}
	check(t, l.Reopen())
	d.Name = "split"
	Emit(t.Context(), m, &d)
	check(t, l.Flush())
	for _, path := range []string{cfg.Path, old.path} {
		if n := strings.Count(readFile(t, path), "event=split "); n != 1 {
			t.Fatalf("split destination %q received %d", path, n)
		}
	}
}

func TestPanicReleasesAttachment(t *testing.T) {
	l := New()
	check(t, l.Configure(Config{Console: ConsoleOff}))
	detach, err := l.Attach(Route{Func: func(*Record) { panic("callback") }})
	check(t, err)
	func() {
		defer func() {
			if recover() != "callback" {
				t.Error("missing panic")
			}
		}()
		l.Module("m").Info("panic")
	}()
	check(t, detach())
	check(t, l.Close())
}

func TestEntrySuppressedRoundTrip(t *testing.T) {
	l := newBuffered(t, "info")
	d := Def{Name: "sample", Sample: &Sampling{First: 1, Thereafter: 2}}
	for range 3 {
		Emit(t.Context(), l.Module("m"), &d, "suppressed", "user")
	}
	entries := l.Memory().Take()
	if len(entries) != 2 {
		t.Fatal(entries)
	}
	for i, e := range entries {
		b, err := json.Marshal(e)
		check(t, err)
		var copyEntry Entry
		check(t, json.Unmarshal(b, &copyEntry))
		r := copyEntry.Record()
		line := string(JSONEncoder{}.Encode(nil, &r))
		if !strings.Contains(line, "\"_suppressed\":\"user\"") || strings.Contains(line, "\"suppressed\":1") != (i == 1) {
			t.Fatal(line)
		}
	}
}

func TestLimitFieldPriority(t *testing.T) {
	for _, source := range []string{"context", "config", "group"} {
		t.Run(source, func(t *testing.T) {
			l := New()
			t.Cleanup(func() { check(t, l.Close()) })
			d := Def{Name: "limit", Limit: "id"}
			for _, id := range []string{"a", "b", "a"} {
				cfg := Config{Console: ConsoleOff, Memory: 4}
				switch source {
				case "config":
					cfg.Attrs = []slog.Attr{slog.String("id", id)}
				case "context":
					cfg.ContextAppend = func(_ context.Context, dst []slog.Attr) []slog.Attr { return append(dst, slog.String("id", id)) }
				case "group":
					cfg.Attrs = []slog.Attr{slog.String("id", "constant")}
				}
				check(t, l.Configure(cfg))
				m := l.Module("m").WithGroup("g")
				if source == "group" {
					Emit(t.Context(), m, &d, "id", id)
				} else {
					Emit(t.Context(), m, &d)
				}
			}
			if got := len(l.Memory().Take()); got != 2 {
				t.Fatal(got)
			}
		})
	}
}

func TestCallerOwnedQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		block := make(chan struct{})
		q := Async(&blockingOut{block: block}, 1)
		l := New()
		check(t, l.Configure(Config{Console: ConsoleOff, Routes: []Route{{Output: q}}}))
		m := l.Module("m")
		m.Info("writing")
		synctest.Wait()
		m.Info("queued")
		m.Info("dropped")
		if q.Dropped() != 1 || l.Dropped() != 0 {
			t.Fatal(q.Dropped(), l.Dropped())
		}
		close(block)
		check(t, l.Close())
		check(t, q.Write(nil))
		check(t, q.Close())
	})
}

// Hold a reader on a published archive until the old configuration is draining.
// Unrelated paths must remain usable while the last lease waits for compression.
func TestArchiveDuringRetirement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		rot := Rotation{Path: filepath.Join(dir, "events.gz"), MaxSize: 1, Compress: true}
		lease, err := reserveRotating(rot)
		check(t, err)
		defer func() { check(t, lease.Close()) }()
		l := New()
		defer func() { check(t, l.Close()) }()
		opened, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		onceOpened := sync.OnceFunc(func() { close(opened) })
		lease.file.Load().activate(func(name string, mode os.FileMode) (string, error) {
			f, e := os.Open(name)
			onceOpened()
			if e != nil {
				return "", e
			}
			<-release
			if e = f.Close(); e != nil {
				return "", e
			}
			return compressFile(name, mode)
		})
		check(t, l.Configure(Config{Console: ConsoleOff, File: rot, SecurityFile: rot}))
		check(t, lease.Close())
		m := l.Module("m")
		m.Info("first")
		m.Info("second")
		<-opened
		// Another rotation on the same path must finish before compression resumes.
		m.Info("third")
		check(t, l.Flush())
		if !strings.Contains(readFile(t, rot.Path), "msg=third") {
			t.Fatal("writer did not rotate during compression")
		}
		done := make(chan error, 1)
		go func() { done <- l.Configure(Config{Console: ConsoleOff}) }()
		synctest.Wait()
		if len(done) > 0 {
			t.Error("retirement passed an open archive")
		}
		other, e := OpenRotating(Rotation{Path: filepath.Join(dir, "other.log")})
		if e == nil {
			e = other.Close()
		}
		unblock()
		check(t, e)
		check(t, <-done)
		check(t, l.Close())
	})
}

func TestCloseWaitsForAttachmentRetirement(t *testing.T) {
	for _, first := range []string{"detach", "Close"} {
		t.Run(first, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := New()
				check(t, l.Configure(Config{Console: ConsoleOff}))
				out := &closeCheck{entered: make(chan struct{}), release: make(chan struct{})}
				remove, err := l.Attach(Route{Output: out})
				check(t, err)
				l.Module("m").Info("submitted")
				<-out.entered
				if first == "Close" {
					remove = l.Close
				}
				removed, closed := make(chan error, 1), make(chan error, 1)
				go func() { removed <- remove() }()
				synctest.Wait()
				go func() { closed <- l.Close() }()
				synctest.Wait()
				if len(closed) != 0 {
					t.Error("Close returned while attachment was writing")
				}
				close(out.release)
				check(t, <-removed)
				check(t, <-closed)
				check(t, out.Close())
			})
		})
	}
}

func TestRetirementError(t *testing.T) {
	l := New()
	failure := errors.New("write failed")
	check(t, l.Configure(Config{Console: ConsoleOff, Routes: []Route{{Output: &faultyOutput{writeErr: failure}}}}))
	l.Module("m").Info("old")
	err := l.Configure(Config{Console: ConsoleOff, Memory: 1})
	if !errors.Is(err, ErrRetirement) || !errors.Is(err, failure) {
		t.Fatalf("Configure: %v", err)
	}
	l.Module("m").Info("new")
	if got := l.Memory().Take(); len(got) != 1 || got[0].Msg != "new" {
		t.Fatal(got)
	}
	check(t, l.Flush())
	check(t, l.Close())
}
