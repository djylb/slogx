# AGENTS.md

- Before you finish, `gofmt -l .` must print nothing and `go vet ./...` and
  `go test -race ./...` must pass.
- Standard library only, also for tests.
- Callers only ever hold a `*slog.Logger`; new features are configuration,
  routes, encoders or outputs, never a second logging API.
- The text and JSON encoders are a format contract: fixed fields first
  (time, level, event, module), msg after the attributes, one line per
  record, no input can start a new line. Keys and order only grow.
- Logging never blocks on a slow consumer: queues are bounded and drop with
  a count.
- An enabled line does not allocate (`go test -bench . -benchmem`): records
  and buffers are pooled, so routes, encoders and outputs never keep them.
- Fuzz the encoders (`go test -fuzz Fuzz...`) after changing them.
- Tests must not leak goroutines or timers.
- Commit subjects are short, English and imperative.
- Never commit host names, IP addresses or credentials.
