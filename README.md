# slogx

Structured logging on `log/slog` for services: one line per event, fixed
fields first, events defined once and routed to a log file, a security log,
an event bus or a live viewer. Standard library only.

Callers only ever hold a `*slog.Logger`. Everything else is configuration.

```
time=2026-11-08T03:12:45.120Z level=WARN event=login_failed module=web src_ip=192.0.2.7 user=admin msg="login failed"
time=2026-11-08T03:12:46.002Z level=INFO module=db rows=3 msg=query
```

## Quick start

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/djylb/slogx"
)

//goland:noinspection GoUnusedGlobalVariable
var loginFailed = slogx.Def{
	Name:     "login_failed",
	Level:    slog.LevelWarn,
	Msg:      "login failed",
	Security: true,
	Limit:    "src_ip", // one line per source IP per minute
}

func main() {
	if err := run(); err != nil {
		slog.Error("logging failed", "err", err)
		os.Exit(1)
	}
}

func run() (err error) {
	logs := slogx.New() // or slogx.Default with slogx.Module
	defer func() { err = errors.Join(err, logs.Close()) }()
	if err := logs.Configure(slogx.Config{
		Level:        "info,db=debug",
		File:         slogx.Rotation{Path: "/var/log/app/app.log", MaxSize: 64 << 20, MaxFiles: 5},
		SecurityFile: slogx.Rotation{Path: "/var/log/app/security.log"},
		Memory:       2000,
	}); err != nil {
		return err
	}

	web := logs.Module("web")
	web.Info("listening", "addr", ":8080")
	slogx.Emit(context.Background(), web, &loginFailed, "src_ip", "192.0.2.7", "user", "admin")
	return nil
}
```

The zero `Config` writes at info level to stderr: coloured on a terminal,
logfmt otherwise. `Configure` replaces the whole pipeline atomically, so
loggers already handed out keep working and a configuration reload needs no
restart.

## Concepts

- **Logs and modules**: a `Logs` is a process's logging; `logs.Module("db")`
  returns a `*slog.Logger` whose lines carry `module=db`. `Handler` gives a
  handler without a module, for `slog.SetDefault`.
- **Def**: an event definition: name, level, default message, whether it is
  a security event or a bus event, and how it is rate limited or sampled.
  `slogx.Emit(ctx, logger, &def, args...)` writes it; an argument keyed
  `slogx.KeyMsg` replaces the default message for one line. The record
  carries its definition, so libraries define their own events without a
  shared registry.
- **Route**: a `Match` and one destination: an `Output` with an `Encoder`, a
  `Memory`, or a callback. Matches come ready-made (`MatchLevels`,
  `MatchSecurity`, `MatchBus`, `MatchMinLevel`) and combine with `MatchAny`
  and `MatchAll`. `Config.Routes` lists routes; `Logs.Attach` adds one for a
  consumer that comes and goes.
- **Levels**: a default level and per-module ones (`info,db=debug,net=warn`).
  `Levels().Override(module, level, ttl)` makes a module more verbose for a
  while and `Clear` ends it early. `Levels` is an `http.Handler` to read and
  change them; mount it behind your own authorization.
- **Memory**: the last entries as structured data, for a viewer: `Query`
  (level, module, event, time, text, incremental by sequence), `Subscribe`
  for new entries, `Take` to hand each entry out once.
- **Sampling**: per event and key (`Def.Limit`), or first-N-then-every-M
  (`Sampling`) for plain lines; the next line that passes carries
  `suppressed=<n>`.
- **slogxtest**: `slogxtest.New` records lines for assertions;
  `slogxtest.ToTest(t)` writes them to the test's own output.

## Format

Text and JSON write the fixed fields first, in the order `time`, `level`,
`event`, `module`, then the line's attributes, then `msg`, `source` and
`stack`. The text (logfmt) and JSON forms carry the same keys. Values are
quoted and escaped so no input can forge a line: an attribute named like a
fixed field (`time`, `level`, `event`, `module`, `msg`, `source`, `stack`,
`suppressed`) is written with a leading `_`, such as `_source`, control characters and bidi overrides
are escaped, a value whose `Error` or `String` panics is written as
`!PANIC`, and `slogx.Redacted` values as `[REDACTED]` (unless `Config.LogSecrets`, a
development-only switch that warns when it turns on, writes them as they
are). `Pretty` is for
people and not meant to be parsed.

## More

- **Outputs**: files that rotate by size or age, with retention,
  compression and permissions. After external rotation, `Logs.Reopen`
  follows each configured path again. Managed queues report dropped lines in a
  `log_dropped` event.
- **Context**: `Config.ContextAppend` appends attributes from a
  `context.Context` into the line's own buffer, such as request ids or the
  pprof labels a component runs under (`PprofLabels`, combined with
  `JoinContext`).
- **Diagnostics**: `Config.AddSource` adds `source=file:line`;
  `Config.StackFrom` adds stacks from a level.
- **Capture**: `HTTPErrorLog` for `http.Server.ErrorLog` turns TLS handshake
  errors into `tls_handshake_failed` events; `StdLogger` and
  `CaptureStdLog` take lines of the standard `log` package.
- **Cost**: pooled text and JSON logging with scalar fields and prebound
  groups is allocation-free after warm-up. Memory history and subscription
  copies, stack and source capture, dynamic group keys and custom values
  can allocate.
- Each byte output uses a bounded queue (1024 lines by default).
  `Flush` waits for preceding writes. `Close` also removes attached routes.
  Removal, Flush and Reopen wait for earlier retirements. Each operation
  returns its own output errors. A Configure error matching `ErrRetirement`
  means the replacement is active. After a route is removed and drained,
  its owner can close its output.
  An explicit `*AsyncOutput` supplies its own queue, drop counter and
  callback. Its next barrier or close returns pending write errors. Logs
  flushes it on removal. `Logs.Dropped` counts managed queue losses per
  destination. To change a live rotation policy, remove the path,
  wait for retirement, then configure the new policy.
  Match functions, encoders and route callbacks run on the logging goroutine.
  Invoke blocking lifecycle operations outside callbacks on the same Logs.
- Operational notices go directly to stderr and to the configured memory
  viewer, including at level off. File-open failures retain the file route's
  filter on stderr. Compatible rotations of the same file share a writer.
  Conflicting active rotation policies return a configuration error.
- Archives live in `.slogx-archives/<full filename>/` beside the target
  file. Their names are UTC timestamps with an optional sequence number and
  gzip suffix. This directory is reserved for rotation. Existing archives
  beside the log stay in place. To include one in retention, confirm which
  log owns it and move it into that log's archive directory using
  `YYYYMMDDTHHMMSS.mmm[-N][.gz]` while the writer is closed.
- Declare event definitions and their sampling policies as immutable.
  Use `Module` to set the module. Attributes supplied with `With` follow
  the same reserved-key rules as inline attributes.

The package examples show each of these in runnable form.

## Filtering security events

With a security file, a filter needs only the fixed fields:

```
failregex = ^time=\S* level=WARN event=login_failed module=\S+ src_ip=<HOST>(\s|$)
```

Pass the address as the event's first argument. An event line writes its own
attributes right after `module=`, ahead of those of `With`, `Config.ContextAppend`
and `Config.Attrs`, so the filter can anchor on it. Do not match
`.*src_ip=`: a later field the client controls, such as a user name, could
carry `src_ip=` of an innocent address. The pattern also accepts an empty
`time=` when a filter strips the timestamp before matching. An event with
a `Limit` writes at most one line per key and minute. Set retry limits
and observation windows in those terms (for example, three lines in ten minutes).

## License

Apache License 2.0
