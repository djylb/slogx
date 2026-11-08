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
	"log/slog"
	"os"

	"github.com/djylb/slogx"
)

var loginFailed = slogx.Def{
	Name:     "login_failed",
	Level:    slog.LevelWarn,
	Msg:      "login failed",
	Security: true,
	Limit:    "src_ip", // one line per source IP per minute
}

func main() {
	logs := slogx.New() // or slogx.Default with slogx.Module
	defer logs.Close()
	err := logs.Configure(slogx.Config{
		Level:        "info,db=debug",
		File:         slogx.Rotation{Path: "/var/log/app/app.log", MaxSize: 64 << 20, MaxFiles: 5},
		SecurityFile: slogx.Rotation{Path: "/var/log/app/security.log"},
		Memory:       2000,
	})
	if err != nil {
		slog.Error("log config", "err", err)
		os.Exit(1)
	}

	web := logs.Module("web")
	web.Info("listening", "addr", ":8080")
	slogx.Emit(context.Background(), web, &loginFailed, "src_ip", "192.0.2.7", "user", "admin")
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

Every encoder writes the fixed fields first, in the order `time`, `level`,
`event`, `module`, then the line's attributes, then `msg`, `source` and
`stack`. The text (logfmt) and JSON forms carry the same keys. Values are
quoted and escaped so no input can forge a line: an attribute named like a
fixed field is written as `_level`, control characters and bidi overrides
are escaped, a value whose `Error` or `String` panics is written as
`!PANIC`, and `slogx.Redacted` values as `[REDACTED]` (unless `Config.LogSecrets`, a
development-only switch that warns when it turns on, writes them as they
are). `Pretty` is for
people and not meant to be parsed.

## More

- **Outputs**: files that rotate by size or age, with retention,
  compression and permissions, and reopen on request for an external
  logrotate (`Logs.Reopen`); `Async` queues lines and drops instead of
  blocking, reporting what it dropped as a `log_dropped` event.
- **Context**: `Config.Context` takes attributes from a `context.Context`,
  such as request ids or the pprof labels a component runs under
  (`PprofLabels`, combined with `JoinContext`).
- **Diagnostics**: `Config.AddSource` adds `source=file:line`;
  `Config.StackFrom` adds stacks from a level.
- **Capture**: `HTTPErrorLog` for `http.Server.ErrorLog` turns TLS handshake
  errors into `tls_handshake_failed` events; `StdLogger` and
  `CaptureStdLog` take lines of the standard `log` package.
- **Cost**: a disabled level costs one atomic load; an enabled line does
  not allocate.

The package examples show each of these in runnable form.

## fail2ban

With a security file, a filter needs only the fixed fields:

```
failregex = ^time=\S* level=WARN event=login_failed .*\bsrc_ip=<HOST>(\s|$)
```

fail2ban strips the timestamp before matching, which leaves `time=` empty,
hence `\S*`. An event with a `Limit` writes at most one line per key and
minute, so each line stands for at least one failure in that minute: set
`maxretry` and `findtime` in those terms (for example 3 lines in 10m).

## License

Apache License 2.0
