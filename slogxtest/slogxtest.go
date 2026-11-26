// Package slogxtest records slogx lines for tests to assert on.
package slogxtest

import (
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"

	"github.com/djylb/slogx"
)

// Line is a recorded line.
type Line struct {
	Level  slog.Level
	Module string
	Event  string
	Msg    string
	Attrs  map[string]string
}

// Recorder keeps every line its route receives.
type Recorder struct {
	mu    sync.Mutex
	lines []Line
}

// Route returns a route that records into r: all lines at or above minLevel,
// whatever the module levels.
func (r *Recorder) Route(minLevel slog.Level) slogx.Route {
	return slogx.Route{Name: "slogxtest", Match: slogx.MatchMinLevel(minLevel), Below: true, Func: r.add}
}

// New returns Logs that write nothing but to the returned Recorder, which
// takes every line at or above trace.
func New() (*slogx.Logs, *Recorder) {
	r := &Recorder{}
	l := slogx.New()
	if err := l.Configure(slogx.Config{Console: slogx.ConsoleOff, Level: "off", Routes: []slogx.Route{r.Route(slogx.LevelTrace)}}); err != nil {
		panic(err)
	}
	return l, r
}

func (r *Recorder) add(rec *slogx.Record) {
	ln := Line{Level: rec.Level, Module: rec.Module, Event: rec.Event, Msg: rec.Msg, Attrs: map[string]string{}}
	for a := range rec.Attributes {
		ln.Attrs[a.Key] = a.Value.String()
	}
	r.mu.Lock()
	r.lines = append(r.lines, ln)
	r.mu.Unlock()
}

// Lines returns the recorded lines.
func (r *Recorder) Lines() []Line {
	r.mu.Lock()
	defer r.mu.Unlock()
	lines := slices.Clone(r.lines)
	for i := range lines {
		lines[i].Attrs = maps.Clone(lines[i].Attrs)
	}
	return lines
}

// Events returns the recorded lines of event name.
func (r *Recorder) Events(name string) []Line {
	var out []Line
	for _, l := range r.Lines() {
		if l.Event == name {
			out = append(out, l)
		}
	}
	return out
}

// Reset forgets the recorded lines.
func (r *Recorder) Reset() {
	r.mu.Lock()
	r.lines = nil
	r.mu.Unlock()
}

// TB is the part of testing.TB that ToTest uses.
type TB interface {
	Output() io.Writer
	Cleanup(f func())
}

// ToTest writes logfmt to the test output at trace and above. Its bounded
// queue counts overflow in Logs.Dropped and drains during test cleanup.
func ToTest(t TB) *slogx.Logs {
	l := slogx.New()
	route := slogx.Route{Name: "test", Match: slogx.MatchMinLevel(slogx.LevelTrace), Below: true, Output: slogx.WriterOutput(t.Output())}
	if err := l.Configure(slogx.Config{Console: slogx.ConsoleOff, Level: "off", Routes: []slogx.Route{route}}); err != nil {
		panic(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}
