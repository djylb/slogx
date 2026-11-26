package leakcheck

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

const leakChild = "SLOGX_LEAKCHECK_CHILD"

func TestMain(m *testing.M) {
	if os.Getenv(leakChild) == "1" {
		os.Exit(m.Run())
	}
	VerifyTestMain(m)
}

func running() map[uint64]bool {
	ids := map[uint64]bool{}
	for _, g := range goroutines() {
		ids[g.id] = true
	}
	return ids
}

func TestFind(t *testing.T) {
	base := running()
	if err := find(0, base); err != nil {
		t.Fatalf("find at rest: %v", err)
	}
	stop := make(chan struct{})
	go func() { <-stop }()
	err := find(10*time.Millisecond, base)
	if err == nil || !strings.Contains(err.Error(), "TestFind") {
		t.Fatalf("find with a running goroutine = %v, want it listed", err)
	}
	close(stop)
	if err := find(settleWait, base); err != nil {
		t.Fatalf("find after stop: %v", err)
	}
}

func TestFindMarksUnreachable(t *testing.T) {
	if runtime.GOARCH == "wasm" {
		t.Skip("isolating a permanently leaked goroutine requires a subprocess")
	}
	if pprof.Lookup("goroutineleak") == nil {
		t.Skip("runtime without leak profile")
	}
	if os.Getenv(leakChild) != "1" {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, exe, "-test.run=^TestFindMarksUnreachable$")
		cmd.Env = append(os.Environ(), leakChild+"=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return
	}
	go func() {
		ch := make(chan int)
		<-ch // no other goroutine can reach ch: leaked for good
	}()
	err := find(10*time.Millisecond, nil)
	if err == nil {
		t.Fatal("find with a leaked goroutine = nil")
	}
	if !strings.Contains(err.Error(), "goroutineleak profile") {
		t.Fatalf("missing goroutineleak profile:\n%v", err)
	}
}

func TestParse(t *testing.T) {
	g, ok := parse("goroutine 7 [chan receive]:\nexample.com/p.f(0x1)\n\t/p/f.go:3 +0x1")
	if !ok || g.id != 7 || g.top != "example.com/p.f" {
		t.Fatalf("parse = %+v, %v", g, ok)
	}
	if _, ok := parse("not a goroutine"); ok {
		t.Fatal("parse accepted garbage")
	}
}
