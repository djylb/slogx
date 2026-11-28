package slogx_test

import (
	"log/slog"
	"slices"
	"testing"

	"github.com/djylb/slogx"
)

func registeredByName(name string) (slogx.Def, bool) {
	i := slices.IndexFunc(slogx.Registered(), func(d slogx.Def) bool { return d.Name == name })
	if i < 0 {
		return slogx.Def{}, false
	}
	return slogx.Registered()[i], true
}

func TestRegister(t *testing.T) {
	d := &slogx.Def{Name: "test.register.audit", Level: slog.LevelInfo, Msg: "audited", Security: true}
	slogx.Register(d)
	got, ok := registeredByName(d.Name)
	if !ok || got.Msg != "audited" || !got.Security {
		t.Fatalf("Registered() lacks %q: %+v", d.Name, got)
	}

	// Registered returns copies: changing the caller's Def changes nothing.
	d.Msg = "changed"
	if got, _ = registeredByName(d.Name); got.Msg != "audited" {
		t.Fatalf("registry aliases the caller's Def: %q", got.Msg)
	}

	// An urgent variant of the same event may differ in level only.
	slogx.Register(&slogx.Def{Name: d.Name, Level: slog.LevelError, Msg: "audited", Security: true})
	if got, _ = registeredByName(d.Name); got.Level != slog.LevelError {
		t.Fatalf("level variant not recorded: %v", got.Level)
	}

	// A second definition that routes elsewhere is a programming error.
	defer func() {
		if recover() == nil {
			t.Fatal("conflicting routing did not panic")
		}
	}()
	slogx.Register(&slogx.Def{Name: d.Name, Level: slog.LevelInfo, Bus: true})
}
