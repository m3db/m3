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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/aggregator/runtime"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
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

func TestOptionsValidateDefault(t *testing.T) {
	o := NewOptions()

	// Validate base options.
	require.Equal(t, defaultMetricPrefix, o.MetricPrefix())
	require.Equal(t, defaultCounterPrefix, o.CounterPrefix())
	require.Equal(t, defaultTimerPrefix, o.TimerPrefix())
	require.Equal(t, defaultGaugePrefix, o.GaugePrefix())
	require.Equal(t, defaultEntryTTL, o.EntryTTL())
	require.Equal(t, defaultEntryCheckInterval, o.EntryCheckInterval())
	require.Equal(t, defaultEntryCheckBatchPercent, o.EntryCheckBatchPercent())
	require.NotNil(t, o.ClockOptions())
	require.NotNil(t, o.InstrumentOptions())
	require.NotNil(t, o.TimeLock())
	require.NotNil(t, o.StreamOptions())
	require.NotNil(t, o.EntryPool())
	require.NotNil(t, o.CounterElemPool())
	require.NotNil(t, o.TimerElemPool())
	require.NotNil(t, o.GaugeElemPool())

	// Validate derived options.
	validateDerivedPrefix(t, o.FullCounterPrefix(), o.MetricPrefix(), o.CounterPrefix())
	validateDerivedPrefix(t, o.FullTimerPrefix(), o.MetricPrefix(), o.TimerPrefix())
	validateDerivedPrefix(t, o.FullGaugePrefix(), o.MetricPrefix(), o.GaugePrefix())
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
	value := cm.NewOptions()
	o := NewOptions().SetStreamOptions(value)
	require.Equal(t, value, o.StreamOptions())
}

func TestSetAdminClient(t *testing.T) {
	var c client.AdminClient = &client.M3MsgClient{}
	o := NewOptions().SetAdminClient(c)
	require.True(t, c == o.AdminClient())
}

func TestSetRuntimeOptionsManager(t *testing.T) {
	value := runtime.NewOptionsManager(runtime.NewOptions())
	o := NewOptions().SetRuntimeOptionsManager(value)
	require.Equal(t, value, o.RuntimeOptionsManager())
}

func TestSetTimeLock(t *testing.T) {
	value := &sync.RWMutex{}
	o := NewOptions().SetTimeLock(value)
	require.Equal(t, value, o.TimeLock())
}

func TestSetFlushHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	h := handler.NewMockHandler(ctrl)
	o := NewOptions().SetFlushHandler(h)
	require.Equal(t, h, o.FlushHandler())
}

func TestSetPassthroughWriter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := writer.NewMockWriter(ctrl)
	o := NewOptions().SetPassthroughWriter(w)
	require.Equal(t, w, o.PassthroughWriter())
}

func TestSetEntryTTL(t *testing.T) {
	value := time.Minute
	o := NewOptions().SetEntryTTL(value)
	require.Equal(t, value, o.EntryTTL())
}

func TestSetMaxAllowedForwardingDelayFn(t *testing.T) {
	value := func(resolution time.Duration, numForwardedTimes int) time.Duration {
		return resolution + time.Second*time.Duration(numForwardedTimes)
	}
	o := NewOptions().SetMaxAllowedForwardingDelayFn(value)
	fn := o.MaxAllowedForwardingDelayFn()
	require.Equal(t, 72*time.Second, fn(time.Minute, 12))
}

func TestSetTimedAggregationBufferPastFn(t *testing.T) {
	value := func(resolution time.Duration) time.Duration {
		return resolution * 2
	}
	o := NewOptions().SetBufferForPastTimedMetricFn(value)
	fn := o.BufferForPastTimedMetricFn()
	require.Equal(t, 2*time.Minute, fn(time.Minute))
}

func TestSetTimedAggregationBufferFutureFn(t *testing.T) {
	o := NewOptions().SetBufferForFutureTimedMetric(3 * time.Minute)
	require.Equal(t, 3*time.Minute, o.BufferForFutureTimedMetric())
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

func TestSetMaxNumCachedSourceSets(t *testing.T) {
	value := 4
	o := NewOptions().SetMaxNumCachedSourceSets(value)
	require.Equal(t, value, o.MaxNumCachedSourceSets())
}

func TestSetDiscardNaNAggregatedValues(t *testing.T) {
	value := false
	o := NewOptions().SetDiscardNaNAggregatedValues(value)
	require.Equal(t, value, o.DiscardNaNAggregatedValues())
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
