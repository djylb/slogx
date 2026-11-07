package slogx

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRegisteredHasBuiltins(t *testing.T) {
	got := map[string]slog.Level{}
	for _, d := range Registered() {
		got[d.Name] = d.Level
	}
	for name, level := range map[string]slog.Level{
		EventLogFallback:        slog.LevelWarn,
		EventLogDropped:         slog.LevelWarn,
		EventTLSHandshakeFailed: slog.LevelDebug,
	} {
		if l, ok := got[name]; !ok || l != level {
			t.Errorf("Registered()[%q] = %v, %v; want %v", name, l, ok, level)
		}
	}
}

func TestMatchAnyAll(t *testing.T) {
	yes := func(*Record, *Levels) bool { return true }
	no := func(*Record, *Levels) bool { return false }
	r := &Record{}
	for _, c := range []struct {
		name string
		m    Match
		want bool
	}{
		{"any()", MatchAny(), false},
		{"any(no,yes)", MatchAny(no, yes), true},
		{"any(no,no)", MatchAny(no, no), false},
		{"all()", MatchAll(), true},
		{"all(yes,yes)", MatchAll(yes, yes), true},
		{"all(yes,no)", MatchAll(yes, no), false},
	} {
		if got := c.m(r, nil); got != c.want {
			t.Errorf("%s = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestCaptureStdLog(t *testing.T) {
	out, flags, prefix := log.Writer(), log.Flags(), log.Prefix()
	defer func() {
		log.SetOutput(out)
		log.SetFlags(flags)
		log.SetPrefix(prefix)
	}()
	l := New()
	defer l.Close()
	if err := l.Configure(Config{Console: ConsoleOff, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	CaptureStdLog(l.Module("lib"), slog.LevelError)
	log.Print("from log")
	es := l.Memory().Take()
	if len(es) != 1 || es[0].Msg != "from log" || es[0].Level != slog.LevelError || es[0].Module != "lib" {
		t.Fatalf("entries = %+v", es)
	}
}

func TestOverrideAfterCloseAndConfigure(t *testing.T) {
	l := New()
	l.Close()
	if err := l.Configure(Config{Console: ConsoleOff}); err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Levels().Override("net", slog.LevelDebug, time.Minute); err != nil {
		t.Fatalf("Override after Close and Configure: %v", err)
	}
}

func TestOverrideFreesExpiredSlot(t *testing.T) {
	l := New()
	defer l.Close()
	if err := l.Configure(Config{Console: ConsoleOff, MaxOverrides: 1}); err != nil {
		t.Fatal(err)
	}
	lv := l.Levels()
	if err := lv.Override("a", slog.LevelDebug, time.Minute); err != nil {
		t.Fatal(err)
	}
	lv.mu.Lock()
	o := lv.overrides["a"]
	o.until = time.Now().Add(-time.Second) // expired, timer not yet run
	lv.overrides["a"] = o
	lv.mu.Unlock()
	if err := lv.Override("b", slog.LevelDebug, time.Minute); err != nil {
		t.Fatalf("Override with only an expired one held: %v", err)
	}
}

func TestNegativeMaxOverrides(t *testing.T) {
	l := New()
	defer l.Close()
	if err := l.Configure(Config{Console: ConsoleOff, MaxOverrides: -1}); err == nil {
		t.Fatal("Configure accepted a negative MaxOverrides")
	}
}

func TestServeHTTPClearWithoutLevel(t *testing.T) {
	l := New()
	defer l.Close()
	if err := l.Configure(Config{Console: ConsoleOff}); err != nil {
		t.Fatal(err)
	}
	lv := l.Levels()
	if err := lv.Override("net", slog.LevelDebug, time.Minute); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"module":"net","ttl":"0"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	lv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("clear = %d %s", w.Code, w.Body)
	}
	if got := lv.Of("net"); got != slog.LevelInfo {
		t.Fatalf("net after clear = %v, want info", got)
	}
}

func TestEmitMsgGroupIsNotMessage(t *testing.T) {
	l := New()
	defer l.Close()
	if err := l.Configure(Config{Console: ConsoleOff, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	d := &Def{Name: "test_group_msg", Level: slog.LevelInfo, Msg: "default"}
	Emit(context.Background(), l.Module("m"), d, slog.Group("msg", "x", 1))
	es := l.Memory().Take()
	if len(es) != 1 || es[0].Msg != "default" {
		t.Fatalf("entries = %+v", es)
	}
}

func TestJoinContextSkipsNil(t *testing.T) {
	f := JoinContext(nil, func(context.Context) []slog.Attr { return []slog.Attr{slog.Int("a", 1)} }, nil)
	if got := f(context.Background()); len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("JoinContext = %v", got)
	}
}
