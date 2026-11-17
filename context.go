package slogx

import (
	"context"
	"log/slog"
	"runtime/pprof"
	"slices"
)

// PprofLabels appends the pprof labels ctx carries to dst as attributes,
// for Config.ContextAppend: a component labelled for profiles and
// tracebacks (such as with pprof.Do) then carries the same names in its log
// lines. Labels are not secret-safe; keep secrets out of them.
func PprofLabels(ctx context.Context, dst []slog.Attr) []slog.Attr {
	pprof.ForLabels(ctx, func(k, v string) bool {
		dst = append(dst, slog.String(k, v))
		return true
	})
	return dst
}

// JoinContext combines context extractors for Config.ContextAppend; the
// attributes of each follow those of the one before. Nil extractors are
// skipped.
func JoinContext(fns ...func(context.Context, []slog.Attr) []slog.Attr) func(context.Context, []slog.Attr) []slog.Attr {
	fns = slices.DeleteFunc(slices.Clone(fns), func(f func(context.Context, []slog.Attr) []slog.Attr) bool { return f == nil })
	return func(ctx context.Context, dst []slog.Attr) []slog.Attr {
		for _, f := range fns {
			dst = f(ctx, dst)
		}
		return dst
	}
}
