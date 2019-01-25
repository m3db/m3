// Copyright (c) 2019 Uber Technologies, Inc.
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

package ingestcarbon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3x/instrument"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	// Keep this value large enough to catch issues like the ingester
	// not copying the name.
	numLinesInTestPacket = 10000
)

// Created by init().
var (
	testMetrics = []testMetric{}
	testPacket  = []byte{}

	testOptions = Options{
		InstrumentOptions: instrument.NewOptions(),
		WorkerPool:        nil, // Set by init().
	}
)

func TestIngesterHandleConn(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)

	var (
		lock = sync.Mutex{}

		found = []testMetric{}
		idx   = 0
	)
	mockDownsamplerAndWriter.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), xtime.Second).DoAndReturn(func(
		_ context.Context,
		tags models.Tags,
		dp ts.Datapoints,
		unit xtime.Unit,
	) interface{} {
		lock.Lock()
		found = append(found, testMetric{
			tags: tags, timestamp: int(dp[0].Timestamp.Unix()), value: dp[0].Value})

		// Make 1 in 10 writes fail to test those paths.
		returnErr := idx%10 == 0
		idx++
		lock.Unlock()

		if returnErr {
			return errors.New("some_error")
		}
		return nil
	}).AnyTimes()

	byteConn := &byteConn{b: bytes.NewBuffer(testPacket)}
	ingester, err := NewIngester(mockDownsamplerAndWriter, testOptions)
	require.NoError(t, err)
	ingester.Handle(byteConn)

	assertTestMetricsAreEqual(t, testMetrics, found)
}

func TestGenerateTagsFromName(t *testing.T) {
	testCases := []struct {
		name         string
		expectedTags []models.Tag
		expectedErr  error
	}{
		{
			name: "foo",
			expectedTags: []models.Tag{
				{Name: []byte("__graphite0__"), Value: []byte("foo")},
			},
		},
		{
			name: "foo.bar.baz",
			expectedTags: []models.Tag{
				{Name: []byte("__graphite0__"), Value: []byte("foo")},
				{Name: []byte("__graphite1__"), Value: []byte("bar")},
				{Name: []byte("__graphite2__"), Value: []byte("baz")},
			},
		},
		{
			name: "foo.bar.baz.",
			expectedTags: []models.Tag{
				{Name: []byte("__graphite0__"), Value: []byte("foo")},
				{Name: []byte("__graphite1__"), Value: []byte("bar")},
				{Name: []byte("__graphite2__"), Value: []byte("baz")},
			},
		},
		{
			name:        "foo..bar..baz..",
			expectedErr: fmt.Errorf("carbon metric: foo..bar..baz.. has duplicate separator"),
		},
		{
			name:        "foo.bar.baz..",
			expectedErr: fmt.Errorf("carbon metric: foo.bar.baz.. has duplicate separator"),
		},
	}

	for _, tc := range testCases {
		tags, err := GenerateTagsFromName([]byte(tc.name))
		if tc.expectedErr != nil {
			require.Equal(t, tc.expectedErr, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, tc.expectedTags, tags.Tags)
	}
}

// byteConn implements the net.Conn interface so that we can test the handler without
// going over the network.
type byteConn struct {
	b      io.Reader
	closed bool
}

func (b *byteConn) Read(buf []byte) (n int, err error) {
	if !b.closed {
		return b.b.Read(buf)
	}

	return 0, io.EOF
}

func (b *byteConn) Write(buf []byte) (n int, err error) {
	panic("not_implemented")
}

func (b *byteConn) Close() error {
	b.closed = true
	return nil
}

func (b *byteConn) LocalAddr() net.Addr {
	panic("not_implemented")
}

func (b *byteConn) RemoteAddr() net.Addr {
	panic("not_implemented")
}

func (b *byteConn) SetDeadline(t time.Time) error {
	panic("not_implemented")
}

func (b *byteConn) SetReadDeadline(t time.Time) error {
	panic("not_implemented")
}

func (b *byteConn) SetWriteDeadline(t time.Time) error {
	panic("not_implemented")
}

type testMetric struct {
	metric    []byte
	tags      models.Tags
	timestamp int
	value     float64
	isValid   bool
}

func assertTestMetricsAreEqual(t *testing.T, a, b []testMetric) {
	require.Equal(t, len(a), len(b))

	sort.Slice(b, func(i, j int) bool {
		return b[i].timestamp < b[j].timestamp
	})

	for i, f := range b {
		require.Equal(t, a[i].tags, f.tags)
		require.Equal(t, a[i].timestamp, f.timestamp)
		require.Equal(t, a[i].value, f.value)
	}
}

func init() {
	var err error
	testOptions.WorkerPool, err = xsync.NewPooledWorkerPool(16, xsync.NewPooledWorkerPoolOptions())
	if err != nil {
		panic(err)
	}
	testOptions.WorkerPool.Init()

	for i := 0; i < numLinesInTestPacket; i++ {
		var metric []byte

		if i%10 == 0 {
			// Make 1 in 10 lines invalid to test the error paths.
			if rand.Intn(2) == 0 {
				// Invalid line altogether.
				line := []byte(fmt.Sprintf("garbage line %d \n", i))
				testPacket = append(testPacket, line...)
				continue
			} else {
				// Valid line, but invalid name (too many separators).
				line := []byte(fmt.Sprintf("test..metric..%d %d %d\n", i, i, i))
				testPacket = append(testPacket, line...)
				continue
			}
		}

		metric = []byte(fmt.Sprintf("test.metric.%d", i))

		tags, err := GenerateTagsFromName(metric)
		if err != nil {
			panic(err)
		}
		testMetrics = append(testMetrics, testMetric{
			metric:    metric,
			tags:      tags,
			timestamp: i,
			value:     float64(i),
		})

		line := []byte(fmt.Sprintf("%s %d %d\n", string(metric), i, i))
		testPacket = append(testPacket, line...)
	}
}
