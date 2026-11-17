package slogx

import (
	"context"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync/atomic"
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

// benchSampledKeys spreads Limit-sampled events over n src_ip values.
func benchSampledKeys(b *testing.B, n int) {
	b.Helper()
	m := benchLogs(b, TextEncoder{}).Module("x")
	ips := make([]string, n)
	for i := range ips {
		ips[i] = "198.51." + strconv.Itoa(i>>8) + "." + strconv.Itoa(i&255)
	}
	var next atomic.Int64
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := int(next.Add(1)) * 7919
		for pb.Next() {
			i = (i + 1) % n
			Emit(context.Background(), m, &testLimit, "src_ip", ips[i])
		}
	})
}

func BenchmarkParallelSampled10Keys(b *testing.B) { benchSampledKeys(b, 10) }
func BenchmarkParallelSampled1MKeys(b *testing.B) { benchSampledKeys(b, 1<<20) }
