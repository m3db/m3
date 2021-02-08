package instrument

import (
	"runtime"
	"testing"
	"time"

	"github.com/uber-go/tally"
)

func BenchmarkSampledTimer(b *testing.B) {
	tm := MustCreateSampledTimer(tally.NoopScope.Timer("test"), 0.5)
	benchRecord(b, tm)
	runtime.KeepAlive(tm)
}

func BenchmarkSampledTimerLowRate(b *testing.B) {
	tm := MustCreateSampledTimer(tally.NoopScope.Timer("test"), 0.5)
	benchRecord(b, tm)
	runtime.KeepAlive(tm)
}

func BenchmarkSampledTimerStopwatch(b *testing.B) {
	tm := MustCreateSampledTimer(tally.NoopScope.Timer("test"), 0.5)
	benchStopwatch(b, tm)
	runtime.KeepAlive(tm)
}

func BenchmarkSampledTimerStopwatchLowRate(b *testing.B) {
	tm := MustCreateSampledTimer(tally.NoopScope.Timer("test"), 0.01)
	benchStopwatch(b, tm)
	runtime.KeepAlive(tm)
}

func benchStopwatch(b *testing.B, tm tally.Timer) {
	b.RunParallel(func(pb *testing.PB) {
		var sw tally.Stopwatch
		for pb.Next() {
			sw = tm.Start()
			sw.Stop()
		}
		runtime.KeepAlive(sw)
	})
}

func benchRecord(b *testing.B, tm tally.Timer) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tm.Record(1 * time.Second)
		}
	})
}
