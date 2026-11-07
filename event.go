package slogx

import (
	"context"
	"log/slog"
	"runtime"
	"sort"
	"sync"
	"time"
)

// Def defines an event: its name, level and where else it goes. An
// application declares its events as package variables, so that the main
// log, the security log and an event bus all take an event from one
// definition. The record carries its Def, so two libraries may define
// events without knowing of each other.
type Def struct {
	Name  string
	Level slog.Level
	// Msg is the message of the event's lines; an Emit argument with the
	// key KeyMsg replaces it for that line.
	Msg string
	// Security events match MatchSecurity (a security log).
	Security bool
	// Bus events match MatchBus (an event bus, a webhook).
	Bus bool
	// Limit, when set, is the field whose value keys rate limiting: one
	// line per event and value per minute, the next carrying how many were
	// suppressed. "*" limits the event as a whole. Sample, when set, is
	// used instead.
	Limit  string
	Sample *Sampling
}

func (d *Def) sampling() (Sampling, string, bool) {
	if d.Sample != nil {
		return *d.Sample, d.Limit, true
	}
	if d.Limit != "" {
		return Sampling{First: 1, Tick: limitWindow}, d.Limit, true
	}
	return Sampling{}, "", false
}

var (
	registryMu sync.RWMutex
	registry   = map[string]*Def{}
)

// Register lists defs for Registered, such as for generating documentation
// of the events an application writes. Logging does not need it.
// Two definitions of one name may differ in level (an urgent variant of an
// event) but not in where the event goes; registering such a pair panics.
func Register(defs ...*Def) {
	registryMu.Lock()
	defer registryMu.Unlock()
	for _, d := range defs {
		if old, ok := registry[d.Name]; ok && old != d && !sameRouting(old, d) {
			panic("slogx: event " + d.Name + " defined twice")
		}
		registry[d.Name] = d
	}
}

func sameRouting(a, b *Def) bool {
	return a.Security == b.Security && a.Bus == b.Bus && a.Limit == b.Limit && (a.Sample == nil) == (b.Sample == nil) && (a.Sample == nil || *a.Sample == *b.Sample)
}

// Registered returns copies of the registered events, by name.
func Registered() []Def {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]Def, 0, len(registry))
	for _, d := range registry {
		c := *d
		if d.Sample != nil {
			c.Sample = new(*d.Sample)
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Names of the events slogx itself writes. Their definitions are fixed;
// Registered lists them.
const (
	// EventLogFallback is written at warn when a log file could not be
	// opened; its lines go to stderr.
	EventLogFallback = "log_fallback"
	// EventLogDropped is written at warn when an asynchronous output dropped
	// lines.
	EventLogDropped = "log_dropped"
	// EventTLSHandshakeFailed is written at debug, limited per src_ip, when
	// net/http reports a failed TLS handshake (HTTPErrorLog).
	EventTLSHandshakeFailed = "tls_handshake_failed"
)

var (
	logFallback        = Def{Name: EventLogFallback, Level: slog.LevelWarn}
	logDropped         = Def{Name: EventLogDropped, Level: slog.LevelWarn}
	tlsHandshakeFailed = Def{Name: EventTLSHandshakeFailed, Level: slog.LevelDebug, Limit: "src_ip"}
)

func init() { Register(&logFallback, &logDropped, &tlsHandshakeFailed) }

// Emit writes event d at its level to l with the given attributes (as
// slog's args), carrying the attributes of ctx (Config.Context). The line's
// message is d.Msg, or the value of the first argument with the key KeyMsg
// ("msg", as a key-value pair or a slog.Attr), which is then not written as
// an attribute. A security or bus event is passed on even when its level is
// filtered out of the main log.
func Emit(ctx context.Context, l *slog.Logger, d *Def, args ...any) {
	emit(ctx, l, d, callerPC(2), args)
}

func callerPC(skip int) uintptr {
	var pcs [1]uintptr
	runtime.Callers(skip+1, pcs[:])
	return pcs[0]
}

func emit(ctx context.Context, l *slog.Logger, d *Def, pc uintptr, args []any) {
	if ctx == nil {
		ctx = context.Background()
	}
	h := l.Handler()
	if !d.Security && !d.Bus && !h.Enabled(ctx, d.Level) {
		return
	}
	msg, before, after := splitMsg(d.Msg, args)
	r := slog.NewRecord(time.Now(), d.Level, msg, pc)
	r.AddAttrs(slog.Any(KeyEvent, defValue{d}))
	r.Add(before...)
	r.Add(after...)
	_ = h.Handle(ctx, r)
}

// splitMsg finds the first argument keyed KeyMsg, walking args as slog
// does, and returns its value as the message and the arguments around it.
func splitMsg(msg string, args []any) (string, []any, []any) {
	for i := 0; i < len(args); i++ {
		switch a := args[i].(type) {
		case slog.Attr:
			if a.Key == KeyMsg && a.Value.Kind() != slog.KindGroup {
				return a.Value.Resolve().String(), args[:i], args[i+1:]
			}
		case string:
			if i+1 >= len(args) {
				return msg, args, nil
			}
			if a == KeyMsg {
				v := args[i+1]
				if s, ok := v.(string); ok {
					return s, args[:i], args[i+2:]
				}
				return slog.AnyValue(v).Resolve().String(), args[:i], args[i+2:]
			}
			i++
		}
	}
	return msg, args, nil
}

// Redacted is a value that never reaches a log: it is written as
// [REDACTED]. Wrap secrets in it when they must travel with other values.
type Redacted string

// LogValue implements slog.LogValuer.
func (Redacted) LogValue() slog.Value { return slog.StringValue("[REDACTED]") }
