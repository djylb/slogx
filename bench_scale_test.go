package slogx

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
)

// BenchmarkParallelText is BenchmarkText from every P at once: the cost
// of the per-line pipeline hold under contention.
func BenchmarkParallelText(b *testing.B) {
	m := benchLogs(b, TextEncoder{}).Module("x")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Info("request done", "client", 42, "ok", true)
		}
	})
}

// BenchmarkParallelSampled hits the sampler from every P.
func BenchmarkParallelSampled(b *testing.B) {
	m := benchLogs(b, TextEncoder{}).Module("x")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Emit(context.Background(), m, &testLimit, "src_ip", "192.0.2.1")
		}
	})
}

func benchFile(b *testing.B, c Config) *slog.Logger {
	b.Helper()
	l := New()
	c.Console = ConsoleOff
	c.File.Path = filepath.Join(b.TempDir(), "a.log")
	c.File.MaxSize = -1
	c.File.FlushInterval = -1
	if err := l.Configure(c); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(l.Close)
	return l.Module("x")
}

func BenchmarkFile(b *testing.B) {
	m := benchFile(b, Config{})
	b.ReportAllocs()
	for b.Loop() {
		m.Info("request done", "client", 42, "ok", true)
	}
}

func BenchmarkAsyncFile(b *testing.B) {
	m := benchFile(b, Config{Async: 1 << 16})
	b.ReportAllocs()
	for b.Loop() {
		m.Info("request done", "client", 42, "ok", true)
	}
}

func BenchmarkBufferedFile(b *testing.B) {
	m := benchFile(b, Config{File: Rotation{BufferSize: 64 << 10}})
	b.ReportAllocs()
	for b.Loop() {
		m.Info("request done", "client", 42, "ok", true)
	}
}

func BenchmarkParallelFile(b *testing.B) {
	m := benchFile(b, Config{})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Info("request done", "client", 42, "ok", true)
		}
	})
}

func BenchmarkParallelBufferedFile(b *testing.B) {
	m := benchFile(b, Config{File: Rotation{BufferSize: 64 << 10}})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Info("request done", "client", 42, "ok", true)
		}
	})
}
