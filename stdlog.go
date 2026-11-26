package slogx

import (
	"bytes"
	"context"
	"errors"
	"log"
	"log/slog"
	"net"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"time"
)

// lineWriter turns each Write of a *log.Logger into one line.
type lineWriter struct {
	l     *slog.Logger
	level slog.Level
	parse func(l *slog.Logger, msg string, pc uintptr) bool
}

func (w lineWriter) Write(p []byte) (int, error) {
	msg := string(bytes.TrimRight(p, "\r\n"))
	pc := outsideLog()
	if w.parse == nil || !w.parse(w.l, msg, pc) {
		ctx := context.Background()
		if w.l.Enabled(ctx, w.level) {
			r := slog.NewRecord(time.Now(), w.level, msg, pc)
			_ = w.l.Handler().Handle(ctx, r)
		}
	}
	return len(p), nil
}

// ownDir is the directory of this package's source, to tell its frames.
var ownDir = func() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file) + "/"
}()

// outsideLog returns the pc of the first caller outside the log package
// and this one, so source= names the code that logged.
func outsideLog() uintptr {
	var pcs [24]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		own := strings.HasPrefix(f.File, ownDir) && !strings.HasSuffix(f.File, "_test.go")
		if !strings.HasPrefix(f.Function, "log.") && !own {
			return f.PC + 1
		}
		if !more {
			return 0
		}
	}
}

// StdLogger returns a *log.Logger that writes each line to l at level.
func StdLogger(l *slog.Logger, level slog.Level) *log.Logger {
	return log.New(lineWriter{l: l, level: level}, "", 0)
}

// CaptureStdLog sends the standard library's default logger, which some
// libraries write to, to l at level: StdLogger for log's default logger.
// The target must write independently of the default log.Logger.
func CaptureStdLog(l *slog.Logger, level slog.Level) error {
	// slog's initial handler (including WithAttrs/WithGroup derivatives)
	// calls log.Output. Its concrete type is private to log/slog.
	if typ := reflect.TypeOf(l.Handler()); typ.Kind() == reflect.Pointer && typ.Elem().PkgPath() == "log/slog" && typ.Elem().Name() == "defaultHandler" {
		return errors.New("slogx: capture target writes back to the standard logger")
	}
	log.SetFlags(0)
	log.SetPrefix("")
	log.SetOutput(lineWriter{l: l, level: level})
	return nil
}

// HTTPErrorLog returns a logger for http.Server.ErrorLog: TLS handshake
// failures become EventTLSHandshakeFailed events of listener, other lines are
// warnings.
func HTTPErrorLog(l *slog.Logger, listener string) *log.Logger {
	return log.New(lineWriter{l: l, level: slog.LevelWarn, parse: func(l *slog.Logger, msg string, pc uintptr) bool {
		return parseTLSHandshake(l, listener, msg, pc)
	}}, "", 0)
}

// parseTLSHandshake reads "http: TLS handshake error from <addr>: <err>".
func parseTLSHandshake(l *slog.Logger, listener, msg string, pc uintptr) bool {
	rest, ok := strings.CutPrefix(msg, "http: TLS handshake error from ")
	if !ok {
		return false
	}
	addr, why, ok := strings.Cut(rest, ": ")
	if !ok || why == "" {
		return false
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	emit(context.Background(), l, &tlsHandshakeFailed, pc, []any{"src_ip", host, "src_port", port, "listener", listener, "reason", tlsReason(why)})
	return true
}

// Reason values of the tls_handshake_failed event.
const (
	reasonEOF        = "eof"
	reasonTimeout    = "timeout"
	reasonBadRecord  = "bad_record"
	reasonNoCert     = "no_cert"
	reasonUnknownSNI = "unknown_sni"
	reasonVersion    = "version"
	reasonCipher     = "cipher"
)

var tlsReasons = [][2]string{
	{"eof", reasonEOF},
	{"timeout", reasonTimeout},
	{"i/o timeout", reasonTimeout},
	{"connection reset", reasonEOF},
	{"first record does not look like", reasonBadRecord},
	{"record overflow", reasonBadRecord},
	{"bad record", reasonBadRecord},
	{"no certificate", reasonNoCert},
	{"no certificates", reasonNoCert},
	{"unknown certificate", reasonNoCert},
	{"unrecognized name", reasonUnknownSNI},
	{"server name", reasonUnknownSNI},
	{"protocol version", reasonVersion},
	{"unsupported versions", reasonVersion},
	{"cipher", reasonCipher},
}

func tlsReason(s string) string {
	s = strings.ToLower(s)
	for _, p := range tlsReasons {
		if strings.Contains(s, p[0]) {
			return p[1]
		}
	}
	return "other"
}
