package slogx

import (
	"errors"
	"log/slog"
	"slices"
)

// Match decides whether a route takes a record. lv is the level table of
// the Logs the route belongs to, with its overrides applied, for a Match
// that compares a record with its module's level (as MatchLevels does);
// other matches may ignore it. A Match must not keep r or lv.
type Match func(r *Record, lv *LevelView) bool

// MatchLevels takes records at or above the level lv gives their module:
// the main log. It is the Match of a Route without one.
func MatchLevels(r *Record, lv *LevelView) bool { return levelAllows(lv.Of(r.Module), r.Level) }

// MatchSecurity takes the events defined as security events, whatever the
// levels.
func MatchSecurity(r *Record, _ *LevelView) bool { return r.Def != nil && r.Def.Security }

// MatchBus takes the events defined as bus events, whatever the levels.
func MatchBus(r *Record, _ *LevelView) bool { return r.Def != nil && r.Def.Bus }

// MatchMinLevel takes records at or above level, whatever the module
// levels. Below admits ordinary records below the module threshold.
// Security and bus events reach the matcher regardless of Below.
func MatchMinLevel(level slog.Level) Match {
	return func(r *Record, _ *LevelView) bool { return levelAllows(level, r.Level) }
}

// MatchAny takes what any non-nil match takes. It snapshots ms.
func MatchAny(ms ...Match) Match {
	ms = slices.DeleteFunc(slices.Clone(ms), func(m Match) bool { return m == nil })
	return func(r *Record, lv *LevelView) bool {
		for _, m := range ms {
			if m(r, lv) {
				return true
			}
		}
		return false
	}
}

// MatchAll takes what every non-nil match takes. It snapshots ms.
func MatchAll(ms ...Match) Match {
	ms = slices.DeleteFunc(slices.Clone(ms), func(m Match) bool { return m == nil })
	return func(r *Record, lv *LevelView) bool {
		for _, m := range ms {
			if !m(r, lv) {
				return false
			}
		}
		return true
	}
}

// Route sends the records Match takes to one destination: encoded to
// Output, kept in Memory, or handed to Func. Exactly one of them is set.
// Logs never closes a route's Output; its owner does, after the
// route is gone (Configure without it, Logs.Close, or detach).
type Route struct {
	Name  string
	Match Match // nil is MatchLevels
	// Below makes the route see records below the level table, for a Match
	// that asks for more than the main log does. Lines are then built even
	// when no other route wants them, which costs.
	Below bool

	Encoder Encoder // nil is TextEncoder
	Output  Output
	Memory  *Memory
	// Func receives the record; it must not keep the record or its Attrs,
	// and must not block. Clone what it needs (Record.Clone).
	Func func(*Record)
}

func (rt *Route) validate() error {
	n := 0
	if rt.Output != nil {
		n++
	}
	if rt.Memory != nil {
		if rt.Memory.Size() == 0 {
			return errors.New("slogx: route requires initialized memory")
		}
		n++
	}
	if rt.Func != nil {
		n++
	}
	if n != 1 {
		return errNoDestination
	}
	return nil
}

func (rt *Route) takes(r *Record, lv *LevelView) bool {
	if rt.Match == nil {
		return MatchLevels(r, lv)
	}
	if !rt.Below && (r.Def == nil || (!r.Def.Security && !r.Def.Bus)) && !MatchLevels(r, lv) {
		return false
	}
	return rt.Match(r, lv)
}

func (rt *Route) deliver(r *Record, buf *[]byte) {
	switch {
	case rt.Output != nil:
		enc := rt.Encoder
		if enc == nil {
			enc = TextEncoder{}
		}
		*buf = enc.Encode((*buf)[:0], r)
		_ = rt.Output.Write(*buf)
	case rt.Memory != nil:
		rt.Memory.add(r)
	case rt.Func != nil:
		rt.Func(r)
	}
}
