package slogx

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

// Entry is a line kept by Memory, detached from the record it came from.
type Entry struct {
	Seq  uint64    `json:"seq"`
	Time time.Time `json:"time"`
	// Level is the slog level, for ordering and filtering; LevelName the
	// name the line carries (LevelName).
	Level     slog.Level `json:"level_value"`
	LevelName string     `json:"level"`
	Module    string     `json:"module,omitempty"`
	Event     string     `json:"event,omitempty"`
	Msg       string     `json:"msg,omitempty"`
	Attrs     []Field    `json:"attrs,omitempty"`
}

// Field is an attribute of an Entry, its value as text.
type Field struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Record returns e as a record with string attributes, for an Encoder:
// enc.Encode(dst, &r).
func (e *Entry) Record() Record {
	r := Record{Time: e.Time, Level: e.Level, Module: e.Module, Event: e.Event, Msg: e.Msg}
	for _, f := range e.Attrs {
		r.Attrs = append(r.Attrs, slog.String(f.Key, f.Value))
	}
	return r
}

// Query selects entries. The zero Query selects everything.
type Query struct {
	After    uint64       // entries with a larger Seq
	Since    time.Time    // at or after
	MinLevel slog.Leveler // at or above, when not nil (nor a nil pointer)
	Modules  []string
	Events   []string
	Text     string // contained in msg or a value, case-insensitive
	Limit    int    // newest Limit entries; zero for all
}

func (q *Query) match(e *Entry) bool {
	if e.Seq <= q.After || (!q.Since.IsZero() && e.Time.Before(q.Since)) {
		return false
	}
	if q.MinLevel != nil && e.Level < q.MinLevel.Level() {
		return false
	}
	if len(q.Modules) > 0 && !slices.Contains(q.Modules, e.Module) {
		return false
	}
	if len(q.Events) > 0 && !slices.Contains(q.Events, e.Event) {
		return false
	}
	if q.Text != "" {
		t := strings.ToLower(q.Text)
		found := strings.Contains(strings.ToLower(e.Msg), t)
		for _, f := range e.Attrs {
			found = found || strings.Contains(strings.ToLower(f.Value), t)
		}
		if !found {
			return false
		}
	}
	return true
}

// Memory keeps the last entries in memory for a viewer, such as a web page:
// Query reads them, Subscribe follows new ones, Take hands out each entry
// once. Use it as the destination of a Route (Route.Memory). A nil *Memory,
// such as Logs.Memory without Config.Memory, keeps nothing: its reads are
// empty and its subscriptions closed.
type Memory struct {
	mu    sync.Mutex
	buf   []Entry
	next  int // where the next entry goes once buf is full
	seq   uint64
	taken uint64
	subs  map[*Subscription]struct{}
}

// NewMemory keeps up to size entries.
func NewMemory(size int) *Memory {
	return &Memory{buf: make([]Entry, 0, max(size, 1))}
}

// Size is how many entries m keeps.
func (m *Memory) Size() int {
	if m == nil {
		return 0
	}
	return cap(m.buf)
}

func (m *Memory) add(r *Record) {
	e := Entry{Time: r.Time, Level: r.Level, LevelName: LevelName(r.Level), Module: r.Module, Event: r.Event, Msg: r.Msg}
	if len(r.Attrs) > 0 {
		e.Attrs = make([]Field, len(r.Attrs))
		for i, a := range r.Attrs {
			e.Attrs[i] = Field{Key: a.Key, Value: string(appendPlain(nil, a.Value))}
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	e.Seq = m.seq
	if len(m.buf) < cap(m.buf) {
		m.buf = append(m.buf, e)
	} else {
		m.buf[m.next] = e
		m.next = (m.next + 1) % len(m.buf)
	}
	for s := range m.subs {
		if s.q.match(&e) {
			select {
			case s.c <- e:
			default:
				s.dropped++
			}
		}
	}
}

// appendPlain writes v as text without logfmt quoting.
func appendPlain(b []byte, v slog.Value) []byte {
	switch v.Kind() {
	case slog.KindString:
		return append(b, v.String()...)
	case slog.KindAny:
		return append(b, valueString(v)...)
	default:
		return appendTextValue(b, v)
	}
}

// Query returns the kept entries q selects, oldest first.
func (m *Memory) Query(q Query) []Entry {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.query(&q)
}

func (m *Memory) query(q *Query) []Entry {
	var out []Entry
	n := len(m.buf)
	for i := range n {
		e := &m.buf[(m.next+i)%n]
		if q.match(e) {
			out = append(out, *e)
		}
	}
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[len(out)-q.Limit:]
	}
	return out
}

// Take returns the kept entries no earlier Take returned, oldest first.
func (m *Memory) Take() []Entry {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.query(&Query{After: m.taken})
	m.taken = m.seq
	return out
}

// Subscription follows new entries.
type Subscription struct {
	m       *Memory
	q       Query
	c       chan Entry
	dropped uint64
	stop    func() bool
}

// C delivers entries; it closes when the subscription ends.
func (s *Subscription) C() <-chan Entry { return s.c }

// Dropped reports how many entries did not fit in the queue.
func (s *Subscription) Dropped() uint64 {
	if s.m == nil {
		return 0
	}
	s.m.mu.Lock()
	defer s.m.mu.Unlock()
	return s.dropped
}

// Close ends the subscription.
func (s *Subscription) Close() {
	if s.m == nil {
		return
	}
	s.m.mu.Lock()
	s.end()
	s.m.mu.Unlock()
	if s.stop != nil {
		s.stop()
	}
}

// end removes s; m.mu is held.
func (s *Subscription) end() {
	if _, ok := s.m.subs[s]; ok {
		delete(s.m.subs, s)
		close(s.c)
	}
}

// Subscribe follows the entries q selects (After, Since and Limit are
// ignored) through a queue of size entries. An entry that does not fit is
// dropped for this subscriber and counted, so a slow viewer never slows
// logging. The subscription ends when ctx is done, Close is called, or the
// Memory is retired by Configure or Logs.Close.
func (m *Memory) Subscribe(ctx context.Context, q Query, size int) *Subscription {
	if m == nil {
		c := make(chan Entry)
		close(c)
		return &Subscription{c: c}
	}
	q.After, q.Since, q.Limit = 0, time.Time{}, 0
	s := &Subscription{m: m, q: q, c: make(chan Entry, max(size, 1))}
	m.mu.Lock()
	if m.subs == nil {
		m.subs = map[*Subscription]struct{}{}
	}
	m.subs[s] = struct{}{}
	m.mu.Unlock()
	if ctx.Done() != nil {
		s.stop = context.AfterFunc(ctx, s.Close)
	}
	return s
}

// closeSubscriptions ends every subscription.
func (m *Memory) closeSubscriptions() {
	m.mu.Lock()
	subs := make([]*Subscription, 0, len(m.subs))
	for s := range m.subs {
		s.end()
		subs = append(subs, s)
	}
	m.mu.Unlock()
	for _, s := range subs {
		if s.stop != nil {
			s.stop()
		}
	}
}

// Text returns the kept entries q selects encoded with enc, for a plain
// download or an application that polls for lines. Pass Query{After: seq}
// to read only what came after the entry seq; to hand each entry out once,
// encode the entries of Take (Entry.Record).
func (m *Memory) Text(q Query, enc Encoder) string {
	var b []byte
	for _, e := range m.Query(q) {
		r := e.Record()
		b = enc.Encode(b, &r)
	}
	return string(b)
}
