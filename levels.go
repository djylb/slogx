package slogx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LevelTrace is below debug, for per-packet detail.
const LevelTrace = slog.Level(-8)

// LevelOff used as a minimum matches no records, including custom levels
// numerically above it. Any enabled threshold is more verbose.
const LevelOff = slog.Level(1 << 20)

// Levels is the level table of a Logs: a default and per-module levels,
// plus temporary overrides that expire. It is safe for concurrent use; reading
// a level costs one atomic load.
type Levels struct {
	cur  atomic.Pointer[LevelView]
	view *atomic.Pointer[LevelView] // current pipeline read view, set under mu
	mu   sync.Mutex                 // writers
	// base is what Set gave; overrides are applied on top of it.
	base      LevelView
	overrides map[string]override
	timer     *time.Timer
	// stopped: the Logs closed; no timer runs again.
	stopped bool
	// maxOverrides is Config.MaxOverrides.
	maxOverrides int
}

type override struct {
	level slog.Level
	until time.Time
}

// LevelView is an immutable snapshot of effective module levels.
type LevelView struct {
	def     slog.Level
	modules map[string]slog.Level
}

// Of returns the effective level of module in this snapshot.
func (t *LevelView) Of(module string) slog.Level {
	if l, ok := t.modules[module]; ok {
		return l
	}
	return t.def
}

func newLevels() *Levels {
	l := &Levels{base: LevelView{def: slog.LevelInfo}}
	l.publish()
	return l
}

// Of returns the level of module now.
func (l *Levels) Of(module string) slog.Level {
	if t := l.cur.Load(); t != nil {
		return t.Of(module)
	}
	return slog.LevelInfo
}

func levelAllows(minimum, level slog.Level) bool { return minimum != LevelOff && level >= minimum }

// moreVerbose combines thresholds, treating off as stricter than any level.
func moreVerbose(a, b slog.Level) slog.Level {
	if a == LevelOff || (b != LevelOff && b < a) {
		return b
	}
	return a
}

// Set replaces the default and per-module levels as spec says,
// "info,db=debug,net=warn". Overrides stay until they expire.
func (l *Levels) Set(spec string) error {
	t, err := parseLevels(spec)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.base = t
	l.publish()
	return nil
}

// String returns the levels as Set reads them, without overrides.
func (l *Levels) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return formatLevels(l.base)
}

// ErrTooManyOverrides is returned by Override beyond Config.MaxOverrides
// modules.
var ErrTooManyOverrides = errors.New("slogx: too many level overrides")

// Override makes module ("" for the default and so every module) log down
// to level for ttl, after which it returns to what Set gave: a temporary
// debug or trace level while investigating. An override only ever makes a
// module more verbose, never quieter than Set made it, and replaces an
// earlier override of the same module. ttl must be positive; Clear ends an
// override early.
func (l *Levels) Override(module string, level slog.Level, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("slogx: override ttl %v is not positive", ttl)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return errors.New("slogx: logs closed")
	}
	limit := l.maxOverrides
	if limit <= 0 {
		limit = 1024
	}
	now := time.Now()
	l.expire(now)
	if _, ok := l.overrides[module]; !ok && len(l.overrides) >= limit {
		return ErrTooManyOverrides
	}
	if l.overrides == nil {
		l.overrides = map[string]override{}
	}
	l.overrides[module] = override{level: level, until: now.Add(ttl)}
	l.publish()
	return nil
}

func (l *Levels) expire(now time.Time) {
	for m, o := range l.overrides {
		if !now.Before(o.until) {
			delete(l.overrides, m) // expired; its timer may not have run yet
		}
	}
}

// Clear ends the override of module now; module returns to what Set gave.
func (l *Levels) Clear(module string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.overrides, module)
	l.publish()
}

// publish builds the effective table; l.mu is held. An override of the
// default ("") applies to every module; a module's own one to it alone.
func (l *Levels) publish() {
	now := time.Now()
	l.expire(now)
	defOverride := LevelOff
	var next time.Time
	for m, r := range l.overrides {
		if next.IsZero() || r.until.Before(next) {
			next = r.until
		}
		if m == "" {
			defOverride = r.level
		}
	}
	t := LevelView{def: moreVerbose(l.base.def, defOverride), modules: make(map[string]slog.Level, len(l.base.modules)+len(l.overrides))}
	for m, v := range l.base.modules {
		t.modules[m] = moreVerbose(v, defOverride)
	}
	for m, r := range l.overrides {
		if m != "" {
			t.modules[m] = moreVerbose(t.Of(m), r.level)
		}
	}
	l.cur.Store(&t)
	if l.view != nil {
		l.view.Store(&t)
	}
	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
	if !next.IsZero() && !l.stopped {
		l.timer = time.AfterFunc(time.Until(next), func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			if !l.stopped {
				l.publish()
			}
		})
	}
}

// configure is Set with a parsed table, and the override limit.
func (l *Levels) configure(t LevelView, maxOverrides int, view *atomic.Pointer[LevelView]) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.base = t
	l.view = view
	l.maxOverrides = maxOverrides
	l.stopped = false // Configure after Close starts afresh
	l.publish()
}

// stop ends the expiry timer and the overrides for good.
func (l *Levels) stop() { l.stopView(nil) }
func (l *Levels) stopView(view *atomic.Pointer[LevelView]) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.view = view
	l.stopped = true
	l.overrides = nil
	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
	l.publish()
}

// ServeHTTP shows the levels on GET, and on PUT or POST takes a JSON body
// {"level": "info,net=debug"} to Set, or {"module": "net", "level":
// "debug", "ttl": "10m"} to Override; {"module": "net", "ttl": "0"}
// clears the module's override, and any other ttl that is not positive is
// an error. The state it shows is {"level": ..., "overrides": {module:
// "debug until <RFC 3339 time>"}}. Mount it behind the application's own
// authorization.
func (l *Levels) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	type state struct {
		Level     string            `json:"level"`
		Overrides map[string]string `json:"overrides,omitempty"`
	}
	switch r.Method {
	case http.MethodGet:
	case http.MethodPut, http.MethodPost:
		// A JSON body cannot be sent cross-site without a preflight, which
		// keeps a browser of an authorized user from being used (CSRF).
		if mt, _, _ := strings.Cut(r.Header.Get("Content-Type"), ";"); strings.TrimSpace(mt) != "application/json" {
			http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
			return
		}
		var req struct {
			Module *string `json:"module"`
			Level  string  `json:"level"`
			TTL    *string `json:"ttl"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if dec.Decode(new(any)) != io.EOF {
			http.Error(w, "unexpected trailing input", http.StatusBadRequest)
			return
		}
		var err error
		var ttlText string
		if req.TTL != nil {
			ttlText = strings.TrimSpace(*req.TTL)
		}
		switch {
		case req.Module == nil && req.TTL != nil:
			err = errors.New("ttl requires module")
		case req.Module != nil && ttlText == "0" && req.Level != "":
			err = errors.New("clear takes no level")
		case req.Module != nil && ttlText == "0":
			l.Clear(*req.Module)
		case strings.TrimSpace(req.Level) == "":
			err = errors.New("level is required")
		case req.Module == nil:
			err = l.Set(req.Level)
		default:
			var lv slog.Level
			var ttl time.Duration
			if lv, err = ParseLevel(req.Level); err == nil {
				ttl, err = time.ParseDuration(ttlText)
			}
			if err == nil {
				err = l.Override(*req.Module, lv, ttl)
			}
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	default:
		w.Header().Set("Allow", "GET, PUT, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	l.mu.Lock()
	st := state{Level: formatLevels(l.base)}
	for m, rs := range l.overrides {
		if st.Overrides == nil {
			st.Overrides = map[string]string{}
		}
		st.Overrides[m] = levelWord(rs.level) + " until " + rs.until.UTC().Format(time.RFC3339)
	}
	l.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	b, err := json.Marshal(st)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(append(b, '\n'))
}

// ParseLevel reads one level name: trace, debug, info, warn, error, off,
// and the syslog-style names and numbers earlier configurations used.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace", "7":
		return LevelTrace, nil
	case "debug", "6":
		return slog.LevelDebug, nil
	case "", "info", "notice", "informational", "5":
		return slog.LevelInfo, nil
	case "warn", "warning", "4":
		return slog.LevelWarn, nil
	case "error", "fatal", "panic", "critical", "alert", "emergency", "3", "2", "1":
		return slog.LevelError, nil
	case "off", "disabled", "none", "0":
		return LevelOff, nil
	}
	return 0, fmt.Errorf("unknown log level %q", s)
}

func parseLevels(spec string) (LevelView, error) {
	t := LevelView{def: slog.LevelInfo}
	for part := range strings.SplitSeq(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		mod, name, ok := strings.Cut(part, "=")
		if !ok {
			l, err := ParseLevel(part)
			if err != nil {
				return LevelView{}, err
			}
			t.def = l
			continue
		}
		if strings.TrimSpace(name) == "" {
			return LevelView{}, errors.New("missing module level")
		}
		l, err := ParseLevel(name)
		if err != nil {
			return LevelView{}, err
		}
		mod = strings.TrimSpace(mod)
		if mod == "" {
			return LevelView{}, errors.New("log level without a module: " + part)
		}
		if t.modules == nil {
			t.modules = map[string]slog.Level{}
		}
		t.modules[mod] = l
	}
	return t, nil
}

func levelWord(l slog.Level) string {
	if l == LevelOff {
		return "off"
	}
	return strings.ToLower(LevelName(l))
}

func formatLevels(t LevelView) string {
	parts := make([]string, 0, 1+len(t.modules))
	parts = append(parts, levelWord(t.def))
	mods := make([]string, 0, len(t.modules))
	for m := range t.modules {
		mods = append(mods, m)
	}
	sort.Strings(mods)
	for _, m := range mods {
		parts = append(parts, m+"="+levelWord(t.modules[m]))
	}
	return strings.Join(parts, ",")
}
