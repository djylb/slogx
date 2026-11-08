// Package slogx is structured logging on log/slog: one line per event, the
// fixed fields first in the order time, level, event, module, then the
// line's attributes, then msg. It adds to slog what an application needs around it: logfmt,
// JSON and terminal encoders; per-module levels with temporary overrides;
// event definitions shared by the log, a security log and an event bus;
// rate limiting and sampling; routes to files, rotating files, an
// in-memory viewer and callbacks; and capture of net/http and the standard
// library's log.
//
// Callers only ever hold a *slog.Logger, from Logs.Module.
package slogx

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// handler is the slog.Handler of a module logger.
type handler struct {
	logs   *Logs
	module string
	attrs  []slog.Attr // with group prefixes applied
	group  string      // "a.b." for WithGroup("a").WithGroup("b")
}

func (h *handler) Enabled(_ context.Context, l slog.Level) bool {
	return h.logs.enabled(h.module, l)
}

func (h *handler) Handle(ctx context.Context, sr slog.Record) error {
	p := h.logs.acquire()
	defer p.mu.RUnlock()
	rp := recordPool.Get().(*Record)
	defer putRecord(rp)
	r := rp
	*r = Record{Time: sr.Time, Level: sr.Level, Module: h.module, Msg: sr.Message, Attrs: r.Attrs[:0], secrets: h.logs.secrets.Load()}
	if r.Time.IsZero() {
		r.Time = time.Now()
	}
	// Attributes go in slog's order: With, the context, the line's own. An
	// event (Emit) puts its own first instead, so its first field directly
	// follows module= and a filter can anchor on it (see the README on
	// fail2ban). Config.Attrs come last on every line.
	own := func() {
		sr.Attrs(func(a slog.Attr) bool {
			if h.group != "" && a.Key == KeyEvent {
				// an event set inside a group is still the event
				r.add(a, "")
				return true
			}
			r.add(a, h.group)
			return true
		})
	}
	event := isEvent(sr)
	if event {
		own()
	}
	for _, a := range h.attrs {
		r.add(a, "")
	}
	if p.ctxAttrs != nil && ctx != nil {
		for _, a := range p.ctxAttrs(ctx) {
			r.add(a, "")
		}
	}
	if !event {
		own()
	}
	for _, a := range p.attrs {
		r.add(a, "")
	}
	if p.addSource {
		r.Source = source(sr.PC)
	}
	if p.stackFrom != nil && r.Level >= p.stackFrom.Level() {
		r.Stack = stack()
	}
	h.logs.dispatch(p, r)
	return nil
}

func (h *handler) WithAttrs(as []slog.Attr) slog.Handler {
	n := *h
	n.attrs = append([]slog.Attr(nil), h.attrs...)
	for _, a := range as {
		if h.group == "" && a.Key == KeyModule {
			n.module = a.Value.Resolve().String()
			continue
		}
		if h.group != "" {
			a = slog.Group(h.group[:len(h.group)-1], a)
		}
		n.attrs = append(n.attrs, a)
	}
	return &n
}

func (h *handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	n := *h
	n.group = h.group + name + "."
	return &n
}

// Records are reused: routes, encoders and outputs must not keep them.
var recordPool = sync.Pool{New: func() any { return &Record{Attrs: make([]slog.Attr, 0, 16)} }}

func putRecord(r *Record) {
	if cap(r.Attrs) > 256 {
		return
	}
	clear(r.Attrs)
	*r = Record{Attrs: r.Attrs[:0]}
	recordPool.Put(r)
}

// isEvent reports a record of Emit, whose first attribute is its event.
func isEvent(sr slog.Record) bool {
	event := false
	sr.Attrs(func(a slog.Attr) bool {
		if a.Key == KeyEvent && a.Value.Kind() == slog.KindLogValuer {
			_, event = a.Value.Any().(defValue)
		}
		return false
	})
	return event
}
