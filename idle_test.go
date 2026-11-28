package slogx

import (
	"testing"

	"github.com/djylb/slogx/internal/leakcheck"
)

// The console queue of an unconfigured Logs, Default included, holds no
// goroutine once its lines are written, so callers' leak checks need no
// exception for it.
func TestConsoleQueueExitsWhenIdle(t *testing.T) {
	Module("idle").Info("first")
	l := New()
	l.Module("idle").Info("second")
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := leakcheck.Find(); err != nil {
		t.Fatal(err)
	}
	l.Module("idle").Info("after idle")
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := leakcheck.Find(); err != nil {
		t.Fatal(err)
	}
}
