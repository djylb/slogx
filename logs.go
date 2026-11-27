package slogx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Keys of the fixed fields. An attribute with one of these keys is written
// as "_" + key, so no attribute can stand in for a fixed field.
const (
	KeyTime       = "time"
	KeyLevel      = "level"
	KeyEvent      = "event"
	KeyModule     = "module"
	KeyMsg        = "msg"
	KeySource     = "source"
	KeyStack      = "stack"
	KeySuppressed = "suppressed"
)

// Console is where Config writes its console lines.
type Console int

const (
	// ConsoleStderr writes the console to os.Stderr.
	ConsoleStderr Console = iota
	// ConsoleStdout writes the console to os.Stdout.
	ConsoleStdout
	// ConsoleOff writes no console.
	ConsoleOff
)

// Config says where and how lines go. The zero Config writes at info level
// to stderr: coloured Pretty on a terminal, logfmt otherwise.
//
// The fields up to Routes are shorthands for the common routes: a console,
// a file, a security file and an in-memory viewer, all but the security
// file taking the lines the level table lets through. Routes adds any
// others.
type Config struct {
	// Console is where the console goes; zero is stderr.
	Console Console
	// Format of the console and the files; zero is Auto.
	Format Format
	// Level is a default level and per-module ones, "info,db=debug".
	Level string
	// MaxOverrides bounds how many modules Levels.Override may override at
	// once; zero is 1024. Lowering it keeps the overrides already made.
	MaxOverrides int
	// File, when its Path is set, is a log file taking the same lines as
	// the console. A file that cannot be opened falls back to stderr, even
	// with ConsoleOff.
	File Rotation
	// SecurityFile, when set, receives every security event.
	SecurityFile Rotation
	// Memory, when positive, keeps that many entries for Logs.Memory. The
	// Memory survives a Configure with the same size, subscribers included.
	Memory int
	// Async sizes each configured output queue in lines. The default is 1024.
	// Full queues drop and count lines.
	Async int
	// Routes are further destinations.
	Routes []Route

	// AddSource adds source=file:line to every line.
	AddSource bool
	// LogSecrets writes Redacted values as they are instead of
	// [REDACTED], for debugging during development only; never set it in
	// production. Turning it on writes an EventLogSecrets warning.
	// Logs.SetLogSecrets switches it without a Configure.
	LogSecrets bool
	// StackFrom, when not nil, adds the stack to lines at or above its
	// level, such as slog.LevelError. As with slog.HandlerOptions.Level, a
	// nil pointer inside the interface is not allowed.
	StackFrom slog.Leveler
	// Attrs are added to every line, events included, after all its other
	// attributes: such as instance= or host= to tell apart processes
	// writing to one place. A key of a fixed field gets a leading "_" like
	// any attribute's.
	Attrs []slog.Attr
	// ContextAppend appends to dst the attributes a context carries, such
	// as a request id, for lines logged with one, and returns the extended
	// slice, as append does. dst belongs to the line being written: append
	// to it, do not keep it. Appending into the line's own buffer lets an
	// extractor run without allocating.
	ContextAppend func(ctx context.Context, dst []slog.Attr) []slog.Attr
	// Sample, when set, samples plain lines (not events) per module,
	// level and message.
	Sample *Sampling
}

// Logs is a process's logging. Its loggers keep working across Configure.
type Logs struct {
	levels   *Levels
	pipe     atomic.Pointer[pipeline]
	attached atomic.Pointer[[]*attachment]
	// attachedBelow: an attached route sees lines below the level table.
	attachedBelow atomic.Bool
	once          sync.Once
	dropped       atomic.Uint64
	writeErrors   atomic.Uint64
	sampler       *sampler
	tail          <-chan struct{} // last published management operation, guarded by mu
	mu            sync.Mutex      // Configure, Close, Attach
}

// pipeline is one configuration, replaced whole. Lines in flight are
// counted in inflight; the pipeline's outputs close only once they are
// done.
type pipeline struct {
	levels    atomic.Pointer[LevelView]
	routes    []Route
	closers   []Output
	inflight  inflight
	memory    *Memory
	stackFrom slog.Leveler
	ctxAppend func(context.Context, []slog.Attr) []slog.Attr
	attrs     []slog.Attr // Config.Attrs
	sample    *Sampling
	secrets   atomic.Bool
	below     bool // a route sees lines below the level table
	addSource bool
}

func newPipeline() *pipeline {
	return &pipeline{}
}

const defaultQueueSize = 1024

func (l *Logs) consolePipeline(idleExit bool) *pipeline {
	// The queue borrows stderr, which WriterOutput leaves open.
	//goland:noinspection GoResourceLeak
	q := l.queue(WriterOutput(os.Stderr), defaultQueueSize, "console")
	q.idleExit = idleExit
	return &pipeline{routes: []Route{{Name: "console", Output: q}}, closers: []Output{q}}
}

// New returns Logs that write logfmt at info level to stderr until
// Configure.
func New() *Logs {
	l := new(Logs)
	l.init()
	return l
}

func (l *Logs) init() {
	l.once.Do(func() {
		l.levels = newLevels()
		l.sampler = newSamplerN(0)
		p := l.consolePipeline(false)
		l.levels.view = &p.levels
		p.levels.Store(l.levels.cur.Load())
		l.pipe.Store(p)
		l.attached.Store(new([]*attachment))
	})
}

// Default is the Logs that Module uses.
var Default = New()

// Module returns a logger of Default whose lines carry module=name.
func Module(name string) *slog.Logger { return Default.Module(name) }

// Module returns a logger whose lines carry module=name.
func (l *Logs) Module(name string) *slog.Logger {
	l.init()
	return slog.New(&handler{logs: l, module: name})
}

// Handler returns a handler with no module, for slog.SetDefault or a
// library that takes one.
func (l *Logs) Handler() slog.Handler { l.init(); return &handler{logs: l} }

// Levels returns the level table.
func (l *Logs) Levels() *Levels { l.init(); return l.levels }

// Memory returns the in-memory entries of Config.Memory. Without one it is
// nil, which keeps nothing: reads are empty, subscriptions closed.
func (l *Logs) Memory() *Memory { l.init(); return l.pipe.Load().memory }

// Configure replaces where and how l writes; lines being written finish on
// the old outputs before those close. A file that cannot be opened falls
// back to stderr with a log_fallback line rather than failing; an invalid
// Console, Format, Level, route or sampling is an error and changes
// nothing. Retirement errors are returned after the replacement is active.
func (l *Logs) Configure(c Config) error {
	l.init()
	lv, err := parseLevels(c.Level)
	if err != nil {
		return err
	}
	if c.Console < ConsoleStderr || c.Console > ConsoleOff {
		return fmt.Errorf("slogx: unknown console %d", c.Console)
	}
	if c.MaxOverrides < 0 {
		return fmt.Errorf("slogx: negative MaxOverrides %d", c.MaxOverrides)
	}
	if c.Format < Auto || c.Format > Pretty {
		return fmt.Errorf("slogx: unknown format %d", c.Format)
	}
	for i := range c.Routes {
		if err = c.Routes[i].validate(); err != nil {
			return err
		}
	}
	if c.Sample != nil {
		if err = c.Sample.validate(); err != nil {
			return err
		}
		c.Sample = new(*c.Sample)
	}
	p := newPipeline()
	p.addSource, p.stackFrom, p.ctxAppend, p.sample, p.attrs = c.AddSource, c.StackFrom, c.ContextAppend, c.Sample, slices.Clone(c.Attrs)
	var fallback []string
	size := c.Async
	if size <= 0 {
		size = defaultQueueSize
	}
	enc := c.Format.Encoder()
	var file *Rotating
	stderrLevels, stderrSecurity := c.Console == ConsoleStderr, false
	for _, spec := range []struct {
		name     string
		rotation Rotation
		match    Match
	}{
		{"file", c.File, nil}, {"security", c.SecurityFile, MatchSecurity},
	} {
		if spec.rotation.Path == "" {
			continue
		}
		o, openErr := reserveRotating(spec.rotation)
		if errors.Is(openErr, errRotationConflict) {
			return errors.Join(openErr, retire(p, nil))
		}
		route := Route{Name: spec.name, Match: spec.match, Encoder: enc}
		if openErr != nil {
			fallback = append(fallback, openErr.Error())
			if spec.match == nil {
				stderrLevels = true
			} else {
				stderrSecurity = true
			}
			continue
		}
		if file != nil {
			if file.path == o.path && file.file.Load() == o.file.Load() {
				// file owns the same writer, so this reference cannot be last.
				_ = o.file.Swap(nil).release()
				p.routes[0].Match = MatchAny(MatchSecurity, MatchLevels)
				continue
			}
			// Different configured paths may converge or diverge on Reopen.
			// Send a security event once when the main file already takes it.
			route.Match = func(r *Record, lv *LevelView) bool {
				return MatchSecurity(r, lv) && (file.file.Load() != o.file.Load() || !MatchLevels(r, lv))
			}
		} else {
			file = o
		}
		p.closers = append(p.closers, o)
		route.Output = o
		p.routes = append(p.routes, route)
	}
	for _, o := range p.closers {
		o.(*Rotating).file.Load().activate(compressFile)
	}
	addConsole := func(w *os.File, match Match) {
		ce := enc
		name := "console"
		if w == os.Stderr && c.Console != ConsoleStderr {
			name = "stderr"
		} else if (c.Format == Auto || c.Format == Pretty) && isTerminal(w) {
			ce = PrettyEncoder{Color: true}
		}
		p.routes = append(p.routes, Route{Name: name, Encoder: ce, Output: WriterOutput(w), Match: match})
	}
	if stderrLevels || stderrSecurity {
		var match Match
		if stderrSecurity {
			match = MatchSecurity
			if stderrLevels {
				match = MatchAny(MatchSecurity, MatchLevels)
			}
		}
		addConsole(os.Stderr, match)
	}
	if c.Console == ConsoleStdout {
		addConsole(os.Stdout, nil)
	}
	l.mu.Lock()
	old := l.pipe.Load()
	if c.Memory > 0 {
		if old.memory != nil && old.memory.Size() == c.Memory {
			p.memory = old.memory
		} else {
			p.memory = NewMemory(c.Memory)
		}
		p.routes = append(p.routes, Route{Name: "memory", Memory: p.memory})
	}
	p.routes = append(p.routes, c.Routes...)
	for i := range p.routes {
		r := &p.routes[i]
		if r.Output != nil {
			r.Output = l.routeQueue(r.Output, size, r.Name)
			p.closers = append(p.closers, r.Output)
		}
		p.below = p.below || r.Below
	}
	l.levels.configure(lv, c.MaxOverrides, &p.levels)
	warn := c.LogSecrets && !old.secrets.Load()
	p.secrets.Store(c.LogSecrets)
	l.pipe.Store(p)
	defer l.beginManagement()()
	if warn {
		l.report(&logSecrets)
	}
	for _, reason := range fallback {
		l.report(&logFallback, slog.String("reason", reason))
	}
	if err := retire(old, p.memory); err != nil {
		return errors.Join(ErrRetirement, err)
	}
	return nil
}

// ErrRetirement marks a Configure error after the new configuration became active.
var ErrRetirement = errors.New("slogx: previous configuration failed to drain")

// beginManagement links the operation into publication order, releases mu,
// and waits for its turn. The returned function releases the next operation.
func (l *Logs) beginManagement() func() {
	prior := l.tail
	done := make(chan struct{})
	l.tail = done
	l.mu.Unlock()
	if prior != nil {
		<-prior
	}
	return func() { close(done) }
}

// UnattributedSuppressed counts sampled-out lines no longer attributable to a key.
func (l *Logs) UnattributedSuppressed() uint64 { l.init(); return l.sampler.unattributed.Load() }

// WriteErrors counts managed output write failures and failed stderr notices.
// Caller-supplied AsyncOutputs report their own errors on Flush, Reopen or Close.
func (l *Logs) WriteErrors() uint64 { return l.writeErrors.Load() }

// Dropped counts lines lost to full output queues across configurations.
func (l *Logs) Dropped() uint64 { return l.dropped.Load() }

// A caller-owned async queue already supplies backpressure. Retirement flushes it.
type borrowedQueue struct{ *AsyncOutput }

func (q borrowedQueue) Close() error { return q.Flush() }

func (l *Logs) routeQueue(out Output, size int, name string) Output {
	if a, ok := out.(*AsyncOutput); ok {
		return borrowedQueue{a}
	}
	return l.queue(out, size, name)
}

func (l *Logs) queue(out Output, size int, name string) *AsyncOutput {
	q := newAsync(out, size, false)
	q.total = &l.dropped
	q.failures = &l.writeErrors
	q.OnDrop = func(n uint64) {
		l.report(&logDropped, slog.String("output", name), slog.Uint64("dropped", n))
	}
	return q
}

// report bypasses routing so reporting a full queue cannot refill it.
func (l *Logs) report(d *Def, attrs ...slog.Attr) {
	r := Record{Time: time.Now(), Level: d.Level, Event: d.Name, Module: "slogx", Msg: d.Msg, Attrs: attrs}
	p, c := l.acquire()
	defer p.inflight.leave(c)
	r.secrets = p.secrets.Load()
	r.addCommon(p.attrs)
	if _, err := os.Stderr.Write(TextEncoder{}.Encode(nil, &r)); err != nil {
		l.writeErrors.Add(1)
	}
	if p.memory != nil {
		p.memory.add(&r)
	}
}

// retire waits for the lines in flight on p, then closes its outputs, and
// its Memory unless it lives on in keep.
func retire(p *pipeline, keep *Memory) error {
	var err error
	p.inflight.close()
	for _, o := range slices.Backward(p.closers) {
		err = errors.Join(err, o.Close())
	}
	if p.memory != nil && p.memory != keep {
		p.memory.closeSubscriptions()
	}
	return err
}

// Attach adds a route that stays across Configure until the returned
// function detaches it, for consumers that come and go: an event bus, a
// live viewer. Detach waits for current calls. Invoke it outside the route's callbacks.
func (l *Logs) Attach(r Route) (detach func() error, err error) {
	l.init()
	if err = r.validate(); err != nil {
		return nil, err
	}
	rp := &attachment{route: r}
	if r.Output != nil {
		rp.route.Output = l.routeQueue(r.Output, defaultQueueSize, r.Name)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	cur := *l.attached.Load()
	l.attached.Store(new(append(slices.Clip(cur), rp)))
	l.attachedBelow.Store(l.attachedBelow.Load() || r.Below)
	var once sync.Once
	return func() error {
		once.Do(func() {
			l.mu.Lock()
			cur := *l.attached.Load()
			next := make([]*attachment, 0, len(cur))
			for _, x := range cur {
				if x != rp {
					next = append(next, x)
				}
			}
			below := false
			for _, x := range next {
				below = below || x.route.Below
			}
			l.attached.Store(&next)
			l.attachedBelow.Store(below)
			defer l.beginManagement()()
			err = rp.close()
		})
		return err
	}, nil
}

type attachment struct {
	route     Route
	flight    inflight
	closeOnce sync.Once
	err       error
}

func (a *attachment) close() error {
	a.closeOnce.Do(func() {
		a.flight.close()
		if a.route.Output != nil {
			a.err = a.route.Output.Close()
		}
	})
	return a.err
}

// Reopen flushes and reopens configured and attached outputs.
func (l *Logs) Reopen() error { return l.manage(reopenOutput) }

// Flush drains configured and attached outputs.
func (l *Logs) Flush() error { return l.manage(flushOutput) }
func (l *Logs) manage(op func(Output) error) error {
	l.init()
	l.mu.Lock()
	p := l.pipe.Load()
	attached := *l.attached.Load()
	defer l.beginManagement()()
	var err error
	for _, r := range p.routes {
		err = errors.Join(err, op(r.Output))
	}
	for _, a := range attached {
		err = errors.Join(err, op(a.route.Output))
	}
	return err
}

// Close drains configured and attached routes. Subsequent lines go to stderr.
func (l *Logs) Close() error {
	l.init()
	l.mu.Lock()
	p := l.consolePipeline(true)
	l.levels.stopView(&p.levels)
	old := l.pipe.Swap(p)
	attachments := l.attached.Swap(new([]*attachment))
	l.attachedBelow.Store(false)
	defer l.beginManagement()()
	err := retire(old, nil)
	for _, a := range *attachments {
		err = errors.Join(err, a.close())
	}
	return err
}

// enabled reports whether a line of module at lv may be written anywhere.
func (l *Logs) enabled(module string, lv slog.Level) bool {
	p := l.pipe.Load()
	if p.below || l.attachedBelow.Load() {
		return true
	}
	return levelAllows(p.levels.Load().Of(module), lv)
}

// acquire returns the current pipeline, held until p.inflight.leave(c).
// A pipeline found closed has already been replaced, so the next load
// finds its successor.
func (l *Logs) acquire() (*pipeline, *atomic.Int64) {
	for {
		p := l.pipe.Load()
		if c, ok := p.inflight.enter(); ok {
			return p, c
		}
	}
}

var bufPool = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}

// dispatch samples r and hands it to the routes that take it.
func (l *Logs) dispatch(p *pipeline, r *Record, attached []*attachment) {
	var policy Sampling
	var field string
	var sampled bool
	if r.Def != nil {
		policy, field, sampled = r.Def.sampling()
	} else if p.sample != nil {
		policy, sampled = *p.sample, true
	}
	if sampled {
		n, pass := l.sampler.allow(keyOf(r, field), policy, r.Time)
		if !pass {
			return
		}
		r.Suppressed = n
	}
	lv := p.levels.Load()
	bp := bufPool.Get().(*[]byte)
	defer putBuf(bp)
	for i := range p.routes {
		rt := &p.routes[i]
		if rt.takes(r, lv) {
			rt.deliver(r, bp)
		}
	}
	for _, a := range attached {
		a.dispatch(r, lv, bp)
	}
}

func (a *attachment) dispatch(r *Record, lv *LevelView, bp *[]byte) {
	if c, ok := a.flight.enter(); ok {
		defer a.flight.leave(c)
		if a.route.takes(r, lv) {
			a.route.deliver(r, bp)
		}
	}
}

// putBuf returns bp to bufPool, unless a huge line grew it: keeping that
// would pin its memory for every later small line.
func putBuf(bp *[]byte) {
	if cap(*bp) <= 64<<10 {
		bufPool.Put(bp)
	}
}

var errNoDestination = errors.New("slogx: a route needs exactly one of Output, Memory and Func")

func source(pc uintptr) string {
	if pc == 0 {
		return ""
	}
	fs := runtime.CallersFrames([]uintptr{pc})
	f, _ := fs.Next()
	if f.File == "" {
		return ""
	}
	file := f.File
	if i := strings.LastIndexByte(file, '/'); i >= 0 {
		if j := strings.LastIndexByte(file[:i], '/'); j >= 0 {
			file = file[j+1:]
		}
	}
	return file + ":" + strconv.Itoa(f.Line)
}

func stack() string {
	buf := make([]byte, 16<<10)
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}

// isTerminal reports whether f is a terminal.
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// SetLogSecrets switches Config.LogSecrets at once, for the lines written
// from then on; turning it on writes an EventLogSecrets warning. The next
// Configure sets it again from its Config.
func (l *Logs) SetLogSecrets(on bool) {
	l.init()
	l.mu.Lock()
	changed := l.pipe.Load().secrets.Swap(on) != on
	l.mu.Unlock()
	if changed && on {
		l.report(&logSecrets)
	}
}
