package slogx

import (
	"context"
	"log/slog"
	"time"
)

// Record is a line on its way to the routes: attributes resolved, groups
// flattened into dotted keys, event and module taken out into their fixed
// places. Encoders and outputs may read it; they must not keep it after
// returning, as it is reused.
type Record struct {
	Time   time.Time
	Level  slog.Level
	Module string
	// Event is the event name; Def its definition when the line was
	// written with Emit.
	Event string
	Def   *Def
	// Attrs contains user attributes. Attributes also yields the sampling count.
	Attrs []slog.Attr
	Msg   string
	// Source is "file:line" of the call when Config.AddSource is set.
	Source string
	// Stack is the goroutine stack when the line is at or above
	// Config.StackFrom.
	Stack string
	// Suppressed counts sampled-out lines since the previous emitted line for this key.
	Suppressed uint64

	// ctx is the reused buffer Config.ContextAppend appends into, kept
	// apart from Attrs because each of its attributes is still resolved
	// and flattened into Attrs.
	ctx []slog.Attr

	group      string
	secrets    bool    // Redacted values are written as they are
	normalized *Record // self only on the handler's read-only record
}

// Clone returns a copy of r that may be kept.
func (r *Record) Clone() Record {
	c := *r
	c.Attrs = append([]slog.Attr(nil), r.Attrs...)
	c.ctx = nil
	c.normalized = nil
	return c
}

// Attr returns the value of the attribute key and whether it is present.
func (r *Record) Attr(key string) (slog.Value, bool) {
	for a := range r.Attributes {
		if a.Key == key {
			return a.Value, true
		}
	}
	return slog.Value{}, false
}

func attributeKey(k string) string {
	switch k {
	case KeyTime:
		return "_" + KeyTime
	case KeyLevel:
		return "_" + KeyLevel
	case KeyEvent:
		return "_" + KeyEvent
	case KeyModule:
		return "_" + KeyModule
	case KeyMsg:
		return "_" + KeyMsg
	case KeySource:
		return "_" + KeySource
	case KeyStack:
		return "_" + KeyStack
	case KeySuppressed:
		return "_" + KeySuppressed
	}
	return k
}

// defValue carries a *Def through slog as the value of the event key, so a
// record knows its definition without a lookup by name.
type defValue struct{ d *Def }

func (v defValue) LogValue() slog.Value { return slog.StringValue(v.d.Name) }

// walkAttr flattens static groups at binding time and resolved groups at
// record time. A nil resolver preserves LogValuers for the logging call.
func walkAttr(a slog.Attr, prefix string, resolve func(slog.Value) slog.Value, visit func(slog.Attr, string)) {
	if resolve != nil {
		a.Value = resolve(a.Value)
	}
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() != slog.KindGroup {
		visit(a, prefix)
		return
	}
	if a.Key != "" {
		prefix += a.Key + "."
	}
	for _, child := range a.Value.Group() {
		walkAttr(child, prefix, resolve, visit)
	}
}

func (r *Record) resolve(v slog.Value) slog.Value {
	if r.secrets && v.Kind() == slog.KindLogValuer {
		if s, ok := v.Any().(Secret); ok && s.raw != nil {
			return slog.StringValue(*s.raw)
		}
	}
	return v.Resolve()
}

func (r *Record) add(a slog.Attr, prefix string) {
	walkAttr(a, prefix, r.resolve, func(a slog.Attr, p string) { a.Key = attributeKey(p + a.Key); r.Attrs = append(r.Attrs, a) })
}

// addContext adds the attributes fn appends for ctx.
func (r *Record) addContext(ctx context.Context, fn func(context.Context, []slog.Attr) []slog.Attr) {
	r.ctx = fn(ctx, r.ctx[:0])
	for _, a := range r.ctx {
		r.add(a, "")
	}
}

func (r *Record) addCommon(attrs []slog.Attr) {
	for _, a := range attrs {
		r.add(a, "")
	}
}

// Attributes yields the record's attributes in encoding order, with reserved
// keys escaped and the sampler's numeric count last.
func (r *Record) Attributes(yield func(slog.Attr) bool) {
	for _, a := range r.Attrs {
		if r.normalized != r {
			a.Key = attributeKey(a.Key)
		}
		if !yield(a) {
			return
		}
	}
	if r.Suppressed > 0 {
		yield(slog.Uint64(KeySuppressed, r.Suppressed))
	}
}
