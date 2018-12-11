// Copyright (c) 2018 Uber Technologies, Inc.
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

package m3

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/collector/reporter"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/pool"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/m3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func newTestServer(t *testing.T, ctrl *gomock.Controller) (
	Server,
	*reporter.MockReporter,
	m3.Reporter,
) {
	reporter := reporter.NewMockReporter(ctrl)

	poolOpts := pool.NewObjectPoolOptions().SetSize(1)
	tagEncoderPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(), poolOpts)
	tagEncoderPool.Init()
	tagDecoderPool := serialize.NewTagDecoderPool(serialize.NewTagDecoderOptions(), poolOpts)
	tagDecoderPool.Init()

	server, err := NewServer("127.0.0.1:0", reporter,
		tagEncoderPool, tagDecoderPool, models.NewTagOptions(), zap.NewNop())
	require.NoError(t, err)

	var (
		up = atomic.NewBool(false)
		wg sync.WaitGroup
	)
	wg.Add(1)
	reporter.EXPECT().
		ReportCounter(gomock.Any(), int64(1)).
		Return(nil).
		Do(func(id id.ID, value int64) {
			assert.Equal(t, int64(1), value)
			up.Store(true)
			wg.Done()
		})

	go func() {
		err := server.Serve()
		assert.NoError(t, err)
	}()

	addr, err := server.Address()
	require.NoError(t, err)

	m3Reporter, err := m3.NewReporter(m3.Options{
		HostPorts: []string{addr},
		Service:   "foo",
		Env:       "test",
		Protocol:  protocol,
	})
	require.NoError(t, err)

	go func() {
		for !up.Load() {
			m3Reporter.
				AllocateCounter("bar", map[string]string{"qux": "qax"}).
				ReportCount(1)
			m3Reporter.Flush()
			time.Sleep(100 * time.Millisecond)
		}
	}()

	wg.Wait()

	return server, reporter, m3Reporter
}

func TestServerCounterIntValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	server, reporter, m3Reporter := newTestServer(t, ctrl)
	defer func() {
		assert.NoError(t, m3Reporter.Close())
		assert.NoError(t, server.Close())
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	reporter.EXPECT().
		ReportCounter(gomock.Any(), int64(42)).
		Return(nil).
		Do(func(id id.ID, value int64) {
			wg.Done()
		})

	m3Reporter.
		AllocateCounter("baz", map[string]string{"foo": "bar"}).
		ReportCount(42)
	m3Reporter.Flush()

	wg.Wait()
}

func TestServerGaugeFloatValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	server, reporter, m3Reporter := newTestServer(t, ctrl)
	defer func() {
		assert.NoError(t, m3Reporter.Close())
		assert.NoError(t, server.Close())
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	reporter.EXPECT().
		ReportGauge(gomock.Any(), float64(42)).
		Return(nil).
		Do(func(id id.ID, value float64) {
			wg.Done()
		})

	m3Reporter.
		AllocateGauge("baz", map[string]string{"foo": "bar"}).
		ReportGauge(42)
	m3Reporter.Flush()

	wg.Wait()
}

func TestServerTimerValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	server, reporter, m3Reporter := newTestServer(t, ctrl)
	defer func() {
		assert.NoError(t, m3Reporter.Close())
		assert.NoError(t, server.Close())
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	reporter.EXPECT().
		ReportBatchTimer(gomock.Any(), []float64{float64(42 * time.Millisecond)}).
		Return(nil).
		Do(func(id id.ID, value []float64) {
			wg.Done()
		})

	m3Reporter.
		AllocateTimer("baz", map[string]string{"foo": "bar"}).
		ReportTimer(42 * time.Millisecond)
	m3Reporter.Flush()

	wg.Wait()
}
