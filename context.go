package slogx

import (
	"context"
	"log/slog"
	"runtime/pprof"
	"slices"
)

// PprofLabels returns the pprof labels ctx carries as attributes, for
// Config.Context: a component labelled for profiles and tracebacks (such
// as with pprof.Do) then carries the same names in its log lines. Labels
// are not secret-safe; keep secrets out of them.
func PprofLabels(ctx context.Context) []slog.Attr {
	var out []slog.Attr
	pprof.ForLabels(ctx, func(k, v string) bool {
		out = append(out, slog.String(k, v))
		return true
	})
	return out
}

// JoinContext combines context extractors for Config.Context; the
// attributes of each follow those of the one before.
func JoinContext(fns ...func(context.Context) []slog.Attr) func(context.Context) []slog.Attr {
	fns = slices.DeleteFunc(slices.Clone(fns), func(f func(context.Context) []slog.Attr) bool { return f == nil })
	return func(ctx context.Context) []slog.Attr {
		var out []slog.Attr
		for _, f := range fns {
			out = append(out, f(ctx)...)
		}
		return out
	}
}
