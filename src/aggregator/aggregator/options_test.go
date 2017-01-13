// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package aggregator

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
)

func validateDerivedPrefix(
	t *testing.T,
	expected []byte,
	part1 []byte,
	part2 []byte,
) {
	full := make([]byte, len(part1)+len(part2))
	n := copy(full, part1)
	copy(full[n:], part2)
	require.Equal(t, expected, full)
}

func validateQuantiles(t *testing.T, o Options) {
	require.Equal(t, o.StreamOptions().Quantiles(), o.TimerQuantiles())
	suffixFn := o.TimerQuantileSuffixFn()
	suffixes := o.TimerQuantileSuffixes()
	for i, q := range o.TimerQuantiles() {
		require.Equal(t, suffixFn(q), suffixes[i])
	}
}

func TestOptionsValidateDefault(t *testing.T) {
	o := NewOptions()

	// Validate base options
	require.Equal(t, defaultMetricPrefix, o.MetricPrefix())
	require.Equal(t, defaultCounterPrefix, o.CounterPrefix())
	require.Equal(t, defaultTimerPrefix, o.TimerPrefix())
	require.Equal(t, defaultTimerSumSuffix, o.TimerSumSuffix())
	require.Equal(t, defaultTimerSumSqSuffix, o.TimerSumSqSuffix())
	require.Equal(t, defaultTimerMeanSuffix, o.TimerMeanSuffix())
	require.Equal(t, defaultTimerLowerSuffix, o.TimerLowerSuffix())
	require.Equal(t, defaultTimerUpperSuffix, o.TimerUpperSuffix())
	require.Equal(t, defaultTimerCountSuffix, o.TimerCountSuffix())
	require.Equal(t, defaultTimerStdevSuffix, o.TimerStdevSuffix())
	require.Equal(t, defaultTimerMedianSuffix, o.TimerMedianSuffix())
	require.Equal(t, defaultGaugePrefix, o.GaugePrefix())
	require.Equal(t, defaultMinFlushInterval, o.MinFlushInterval())
	require.Equal(t, defaultMaxFlushSize, o.MaxFlushSize())
	require.Equal(t, defaultEntryTTL, o.EntryTTL())
	require.Equal(t, defaultEntryCheckInterval, o.EntryCheckInterval())
	require.Equal(t, defaultEntryCheckBatchPercent, o.EntryCheckBatchPercent())
	require.NotNil(t, o.TimerQuantileSuffixFn())
	require.NotNil(t, o.FlushFn())
	require.NotNil(t, o.ClockOptions())
	require.NotNil(t, o.InstrumentOptions())
	require.NotNil(t, o.TimeLock())
	require.NotNil(t, o.StreamOptions())
	require.NotNil(t, o.EntryPool())
	require.NotNil(t, o.CounterElemPool())
	require.NotNil(t, o.TimerElemPool())
	require.NotNil(t, o.GaugeElemPool())
	require.NotNil(t, o.BufferedEncoderPool())

	// Validate derived options
	validateDerivedPrefix(t, o.FullCounterPrefix(), o.MetricPrefix(), o.CounterPrefix())
	validateDerivedPrefix(t, o.FullTimerPrefix(), o.MetricPrefix(), o.TimerPrefix())
	validateDerivedPrefix(t, o.FullGaugePrefix(), o.MetricPrefix(), o.GaugePrefix())
	validateQuantiles(t, o)
}

func TestOptionsSetMetricPrefix(t *testing.T) {
	newPrefix := []byte("testMetricPrefix")
	o := NewOptions().SetMetricPrefix(newPrefix)
	require.Equal(t, newPrefix, o.MetricPrefix())
	validateDerivedPrefix(t, o.FullCounterPrefix(), o.MetricPrefix(), o.CounterPrefix())
	validateDerivedPrefix(t, o.FullTimerPrefix(), o.MetricPrefix(), o.TimerPrefix())
	validateDerivedPrefix(t, o.FullGaugePrefix(), o.MetricPrefix(), o.GaugePrefix())
}

func TestOptionsSetCounterPrefix(t *testing.T) {
	newPrefix := []byte("testCounterPrefix")
	o := NewOptions().SetCounterPrefix(newPrefix)
	require.Equal(t, newPrefix, o.CounterPrefix())
	validateDerivedPrefix(t, o.FullCounterPrefix(), o.MetricPrefix(), o.CounterPrefix())
}

func TestOptionsSetTimerPrefix(t *testing.T) {
	newPrefix := []byte("testTimerPrefix")
	o := NewOptions().SetTimerPrefix(newPrefix)
	require.Equal(t, newPrefix, o.TimerPrefix())
	validateDerivedPrefix(t, o.FullTimerPrefix(), o.MetricPrefix(), o.TimerPrefix())
}

func TestOptionsSetTimerSumSuffix(t *testing.T) {
	newSumSuffix := []byte("testTimerSumSuffix")
	o := NewOptions().SetTimerSumSuffix(newSumSuffix)
	require.Equal(t, newSumSuffix, o.TimerSumSuffix())
}

func TestOptionsSetTimerSumSqSuffix(t *testing.T) {
	newSumSqSuffix := []byte("testTimerSumSqSuffix")
	o := NewOptions().SetTimerSumSqSuffix(newSumSqSuffix)
	require.Equal(t, newSumSqSuffix, o.TimerSumSqSuffix())
}

func TestOptionsSetTimerMeanSuffix(t *testing.T) {
	newMeanSuffix := []byte("testTimerMeanSuffix")
	o := NewOptions().SetTimerMeanSuffix(newMeanSuffix)
	require.Equal(t, newMeanSuffix, o.TimerMeanSuffix())
}

func TestOptionsSetTimerLowerSuffix(t *testing.T) {
	newLowerSuffix := []byte("testTimerLowerSuffix")
	o := NewOptions().SetTimerLowerSuffix(newLowerSuffix)
	require.Equal(t, newLowerSuffix, o.TimerLowerSuffix())
}

func TestOptionsSetTimerUpperSuffix(t *testing.T) {
	newUpperSuffix := []byte("testTimerUpperSuffix")
	o := NewOptions().SetTimerUpperSuffix(newUpperSuffix)
	require.Equal(t, newUpperSuffix, o.TimerUpperSuffix())
}

func TestOptionsSetTimerCountSuffix(t *testing.T) {
	newCountSuffix := []byte("testTimerCountSuffix")
	o := NewOptions().SetTimerCountSuffix(newCountSuffix)
	require.Equal(t, newCountSuffix, o.TimerCountSuffix())
}

func TestOptionsSetTimerStdevSuffix(t *testing.T) {
	newStdevSuffix := []byte("testTimerStdevSuffix")
	o := NewOptions().SetTimerStdevSuffix(newStdevSuffix)
	require.Equal(t, newStdevSuffix, o.TimerStdevSuffix())
}

func TestOptionsSetTimerMedianSuffix(t *testing.T) {
	newMedianSuffix := []byte("testTimerMedianSuffix")
	o := NewOptions().SetTimerMedianSuffix(newMedianSuffix)
	require.Equal(t, newMedianSuffix, o.TimerMedianSuffix())
}

func TestOptionsSetTimerQuantileSuffixFn(t *testing.T) {
	fn := func(q float64) []byte { return []byte(fmt.Sprintf("%1.2f", q)) }
	o := NewOptions().SetTimerQuantileSuffixFn(fn)
	require.Equal(t, []byte("0.96"), o.TimerQuantileSuffixFn()(0.9582))
	validateQuantiles(t, o)
}

func TestOptionsSetGaugePrefix(t *testing.T) {
	newPrefix := []byte("testGaugePrefix")
	o := NewOptions().SetGaugePrefix(newPrefix)
	require.Equal(t, newPrefix, o.GaugePrefix())
	validateDerivedPrefix(t, o.FullGaugePrefix(), o.MetricPrefix(), o.GaugePrefix())
}

func TestSetClockOptions(t *testing.T) {
	value := clock.NewOptions()
	o := NewOptions().SetClockOptions(value)
	require.Equal(t, value, o.ClockOptions())
}

func TestSetInstrumentOptions(t *testing.T) {
	value := instrument.NewOptions()
	o := NewOptions().SetInstrumentOptions(value)
	require.Equal(t, value, o.InstrumentOptions())
}

func TestSetStreamOptions(t *testing.T) {
	value := cm.NewOptions().SetQuantiles([]float64{0.1, 0.3, 0.5, 0.95, 0.97, 0.99})
	o := NewOptions().SetStreamOptions(value)
	require.Equal(t, value, o.StreamOptions())
	validateQuantiles(t, o)
}

func TestSetTimeLock(t *testing.T) {
	value := &sync.RWMutex{}
	o := NewOptions().SetTimeLock(value)
	require.Equal(t, value, o.TimeLock())
}

func TestSetMinFlushInterval(t *testing.T) {
	value := time.Second * 15
	o := NewOptions().SetMinFlushInterval(value)
	require.Equal(t, value, o.MinFlushInterval())
}

func TestSetMaxFlushSize(t *testing.T) {
	value := 10000
	o := NewOptions().SetMaxFlushSize(value)
	require.Equal(t, value, o.MaxFlushSize())
}

func TestSetFlushFn(t *testing.T) {
	var b *bytes.Buffer
	buf := msgpack.NewPooledBufferedEncoder(nil)
	value := func(buf msgpack.BufferedEncoder) error {
		b = buf.Buffer
		return nil
	}
	o := NewOptions().SetFlushFn(value)
	require.NoError(t, o.FlushFn()(buf))
	require.Equal(t, b, buf.Buffer)
}

func TestSetEntryTTL(t *testing.T) {
	value := time.Minute
	o := NewOptions().SetEntryTTL(value)
	require.Equal(t, value, o.EntryTTL())
}

func TestSetEntryCheckInterval(t *testing.T) {
	value := time.Minute
	o := NewOptions().SetEntryCheckInterval(value)
	require.Equal(t, value, o.EntryCheckInterval())
}

func TestSetEntryCheckBatchPercent(t *testing.T) {
	value := 0.05
	o := NewOptions().SetEntryCheckBatchPercent(value)
	require.Equal(t, value, o.EntryCheckBatchPercent())
}

func TestSetEntryPool(t *testing.T) {
	value := NewEntryPool(nil)
	o := NewOptions().SetEntryPool(value)
	require.Equal(t, value, o.EntryPool())
}

func TestSetCounterElemPool(t *testing.T) {
	value := NewCounterElemPool(nil)
	o := NewOptions().SetCounterElemPool(value)
	require.Equal(t, value, o.CounterElemPool())
}

func TestSetTimerElemPool(t *testing.T) {
	value := NewTimerElemPool(nil)
	o := NewOptions().SetTimerElemPool(value)
	require.Equal(t, value, o.TimerElemPool())
}

func TestSetGaugeElemPool(t *testing.T) {
	value := NewGaugeElemPool(nil)
	o := NewOptions().SetGaugeElemPool(value)
	require.Equal(t, value, o.GaugeElemPool())
}

func TestSetBufferedEncoderPool(t *testing.T) {
	value := msgpack.NewBufferedEncoderPool(nil)
	o := NewOptions().SetBufferedEncoderPool(value)
	require.Equal(t, value, o.BufferedEncoderPool())
}
