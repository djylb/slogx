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
	attrs  []boundAttr // with group prefixes applied
	group  string      // "a.b." for WithGroup("a").WithGroup("b")
}

func (h *handler) Enabled(_ context.Context, l slog.Level) bool {
	return h.logs.enabled(h.module, l)
}

func (h *handler) Handle(ctx context.Context, sr slog.Record) error {
	attached := *h.logs.attached.Load()
	p, c := h.logs.acquire()
	defer p.inflight.leave(c)
	rp := recordPool.Get().(*Record)
	defer putRecord(rp)
	r := rp
	*r = Record{Time: sr.Time, Level: sr.Level, Module: h.module, Msg: sr.Message, Attrs: r.Attrs[:0], ctx: r.ctx[:0], group: h.group, secrets: p.secrets.Load(), normalized: r}
	if r.Time.IsZero() {
		r.Time = time.Now()
	}
	// Attributes go in slog's order: With, the context, the line's own. An
	// event (Emit) puts its own first instead, so its first field directly
	// follows module= and a filter can anchor on it. Config.Attrs come last.
	r.Def = eventDef(sr)
	if r.Def != nil {
		r.Event = r.Def.Name
	}
	own := func() {
		marker := r.Def != nil
		sr.Attrs(func(a slog.Attr) bool {
			if marker {
				marker = false
			} else {
				r.add(a, h.group)
			}
			return true
		})
	}
	if r.Def != nil {
		own()
	}
	for _, a := range h.attrs {
		r.add(a.attr, a.prefix)
	}
	if p.ctxAppend != nil && ctx != nil {
		r.addContext(ctx, p.ctxAppend)
	}
	if r.Def == nil {
		own()
	}
	r.addCommon(p.attrs)
	if p.addSource {
		r.Source = source(sr.PC)
	}
	if p.stackFrom != nil && levelAllows(p.stackFrom.Level(), r.Level) {
		r.Stack = stack()
	}
	h.logs.dispatch(p, r, attached)
	return nil
}

func (h *handler) WithAttrs(as []slog.Attr) slog.Handler {
	n := *h
	n.attrs = append([]boundAttr(nil), h.attrs...)
	for _, a := range as {
		walkAttr(a, h.group, nil, func(a slog.Attr, prefix string) {
			// A valuer with an empty key may produce an inline group. Preserve
			// its parent prefix until we know whether it resolves to a group.
			if a.Key == "" && a.Value.Kind() == slog.KindLogValuer {
				n.attrs = append(n.attrs, boundAttr{attr: a, prefix: prefix})
			} else {
				a.Key = prefix + a.Key
				n.attrs = append(n.attrs, boundAttr{attr: a})
			}
		})
	}
	return &n
}

type boundAttr struct {
	attr   slog.Attr
	prefix string
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
	if cap(r.Attrs) > 256 || cap(r.ctx) > 256 {
		return
	}
	clear(r.Attrs)
	clear(r.ctx)
	*r = Record{Attrs: r.Attrs[:0], ctx: r.ctx[:0]}
	recordPool.Put(r)
}

// eventDef recognizes only Emit's leading private marker.
func eventDef(sr slog.Record) *Def {
	var d *Def
	sr.Attrs(func(a slog.Attr) bool {
		if a.Key == KeyEvent && a.Value.Kind() == slog.KindLogValuer {
			if marker, ok := a.Value.Any().(defValue); ok {
				d = marker.d
			}
		}
		return false
	})
	return d
}
