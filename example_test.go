package slogx_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/djylb/slogx"
)

// stdout is a route that writes logfmt without the time to os.Stdout, so
// the examples' output is stable.
func stdout(m slogx.Match) slogx.Route {
	return slogx.Route{Match: m, Output: slogx.WriterOutput(os.Stdout), Encoder: noTime{slogx.TextEncoder{}}}
}

func Example() {
	logs := slogx.New()
	_ = logs.Configure(slogx.Config{
		Console: slogx.ConsoleOff,
		Level:   "info,db=debug",
		Routes:  []slogx.Route{stdout(nil)},
	})
	defer logs.Close()

	logs.Module("web").Info("listening", "addr", ":8080")
	logs.Module("db").Debug("query", "rows", 3)
	logs.Module("cache").Debug("hidden")
	// Output:
	// level=INFO module=web addr=:8080 msg=listening
	// level=DEBUG module=db rows=3 msg=query
}

func ExampleEmit() {
	loginFailed := slogx.Def{Name: "login_failed", Level: slog.LevelWarn, Msg: "login failed", Security: true}

	logs := slogx.New()
	_ = logs.Configure(slogx.Config{Console: slogx.ConsoleOff, Routes: []slogx.Route{stdout(nil)}})
	defer logs.Close()

	ctx := context.Background()
	web := logs.Module("web")
	slogx.Emit(ctx, web, &loginFailed, "src_ip", "192.0.2.7", "user", "admin")
	slogx.Emit(ctx, web, &loginFailed, "src_ip", "192.0.2.8", slogx.KeyMsg, "account locked")
	// Output:
	// level=WARN event=login_failed module=web src_ip=192.0.2.7 user=admin msg="login failed"
	// level=WARN event=login_failed module=web src_ip=192.0.2.8 msg="account locked"
}

func ExampleLevels_Override() {
	logs := slogx.New()
	_ = logs.Configure(slogx.Config{Console: slogx.ConsoleOff, Level: "info", Routes: []slogx.Route{stdout(nil)}})
	defer logs.Close()

	db := logs.Module("db")
	db.Debug("before")
	_ = logs.Levels().Override("db", slog.LevelDebug, 10*time.Minute)
	db.Debug("during")
	logs.Levels().Clear("db")
	db.Debug("after")
	// Output:
	// level=DEBUG module=db msg=during
}

func ExampleMemory_Subscribe() {
	logs := slogx.New()
	_ = logs.Configure(slogx.Config{Console: slogx.ConsoleOff, Memory: 100})
	defer logs.Close()

	sub := logs.Memory().Subscribe(context.Background(), slogx.Query{MinLevel: slog.LevelWarn}, 16)
	defer sub.Close()
	logs.Module("web").Info("ignored")
	logs.Module("web").Warn("slow request", "ms", 900)
	e := <-sub.C()
	fmt.Println(e.LevelName, e.Module, e.Msg, e.Attrs[0].Key, e.Attrs[0].Value)
	// Output:
	// WARN web slow request ms 900
}

func ExampleAsync() {
	dir, _ := os.MkdirTemp("", "slogx-example")
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, "app.log")

	// A file rotated at 64 MiB, the last 5 kept gzipped, written through a
	// queue of 1024 lines so a slow disk never slows the caller.
	f, err := slogx.OpenRotating(slogx.Rotation{Path: path, MaxSize: 64 << 20, MaxFiles: 5, Compress: true})
	if err != nil {
		fmt.Println(err)
		return
	}
	out := slogx.Async(f, 1024)
	logs := slogx.New()
	_ = logs.Configure(slogx.Config{
		Console: slogx.ConsoleOff,
		Routes:  []slogx.Route{{Output: out, Encoder: noTime{slogx.TextEncoder{}}}},
	})
	logs.Module("app").Info("started")
	logs.Close()
	_ = out.Close() // a route's Output is its owner's to close: this writes the queue and closes f

	b, _ := os.ReadFile(path)
	fmt.Print(string(b))
	// Output:
	// level=INFO module=app msg=started
}

func ExampleHTTPErrorLog() {
	logs := slogx.New()
	_ = logs.Configure(slogx.Config{Console: slogx.ConsoleOff, Level: "debug", Routes: []slogx.Route{stdout(nil)}})
	defer logs.Close()

	// As http.Server{ErrorLog: errLog} would write it:
	errLog := slogx.HTTPErrorLog(logs.Module("web"), "https")
	errLog.Print("http: TLS handshake error from 192.0.2.9:51234: EOF")
	errLog.Print("http: superfluous response.WriteHeader call")
	// Output:
	// level=DEBUG event=tls_handshake_failed module=web src_ip=192.0.2.9 src_port=51234 listener=https reason=eof
	// level=WARN module=web msg="http: superfluous response.WriteHeader call"
}

func ExampleRoute() {
	keyRevoked := slogx.Def{Name: "key_revoked", Level: slog.LevelInfo, Bus: true}

	logs := slogx.New()
	// The main log takes warnings and above; a bus route takes the bus
	// events whatever their level.
	_ = logs.Configure(slogx.Config{
		Console: slogx.ConsoleOff,
		Level:   "warn",
		Routes: []slogx.Route{stdout(nil), {Name: "bus", Match: slogx.MatchBus, Func: func(r *slogx.Record) {
			fmt.Println("bus:", r.Event)
		}}},
	})
	defer logs.Close()

	slogx.Emit(context.Background(), logs.Module("auth"), &keyRevoked, "key", "k1")
	logs.Module("auth").Warn("clock skew")
	// Output:
	// bus: key_revoked
	// level=WARN module=auth msg="clock skew"
}

// noTime drops the time field so the example output is stable.
type noTime struct{ slogx.Encoder }

func (e noTime) Encode(b []byte, r *slogx.Record) []byte {
	line := e.Encoder.Encode(nil, r)
	if i := strings.IndexByte(string(line), ' '); i >= 0 {
		return append(b, line[i+1:]...)
	}
	return append(b, line...)
}
