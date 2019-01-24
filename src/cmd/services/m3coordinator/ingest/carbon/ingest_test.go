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
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3x/time"
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
)

func TestIngesterHandleConn(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)

	var (
		foundLock = sync.Mutex{}
		found     = []testMetric{}
	)
	mockDownsamplerAndWriter.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), xtime.Second).DoAndReturn(func(
		_ context.Context,
		tags models.Tags,
		dp ts.Datapoints,
		unit xtime.Unit,
	) {
		foundLock.Lock()
		found = append(found, testMetric{
			tags: tags, timestamp: int(dp[0].Timestamp.Unix()), value: dp[0].Value})
		foundLock.Unlock()
	}).Return(nil).AnyTimes()

	byteConn := &byteConn{b: bytes.NewBuffer(testPacket)}
	ingester := NewIngester(mockDownsamplerAndWriter, Options{})
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
				{Name: []byte("__$0__"), Value: []byte("foo")},
			},
		},
		{
			name: "foo.bar.baz",
			expectedTags: []models.Tag{
				{Name: []byte("__$0__"), Value: []byte("foo")},
				{Name: []byte("__$1__"), Value: []byte("bar")},
				{Name: []byte("__$2__"), Value: []byte("baz")},
			},
		},
		{
			name: "foo.bar.baz.",
			expectedTags: []models.Tag{
				{Name: []byte("__$0__"), Value: []byte("foo")},
				{Name: []byte("__$1__"), Value: []byte("bar")},
				{Name: []byte("__$2__"), Value: []byte("baz")},
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
		tags, err := generateTagsFromName([]byte(tc.name))
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
	for i := 0; i < numLinesInTestPacket; i++ {
		metric := []byte(fmt.Sprintf("test_metric_%d", i))

		tags, err := generateTagsFromName(metric)
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
