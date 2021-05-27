// +build integration

// Copyright (c) 2017 Uber Technologies, Inc.
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

package integration

import (
	"os"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

type readWriteTZCase struct {
	namespace  string
	id         string
	datapoints []readWriteTZDP
}

type readWriteTZDP struct {
	value     float64
	timestamp xtime.UnixNano
}

// Make sure that everything works properly end-to-end even if the client issues
// a write in a timezone other than that of the server.
func TestWriteReadTimezone(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Ensure that the test is running with the local timezone set to US/Pacific.
	// Note that we do this instead of just manipulating the NowFn on the test
	// setup because there are ways to end up with a time object with a non-UTC
	// timezone besides calling time.Now(). For example, converting a unix
	// timestamp into a time.Time object will automatically associate the machine's
	// local timezone.
	os.Setenv("TZ", "US/Pacific")
	name, offset := time.Now().Zone()
	// The zone name will be PST or PDT depending on whether daylight savings
	// is currently in effect
	if name == "PDT" {
		require.Equal(t, offset, -25200)
	} else if name == "PST" {
		require.Equal(t, offset, -28800)
	} else {
		t.Fatalf("Zone should be PDT or PST, but was: %s", name)
	}

	// Load locations that we'll need later in the tests
	pacificLocation, err := time.LoadLocation("US/Pacific")
	require.NoError(t, err)
	nyLocation, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// Setup / start server
	opts := NewTestOptions(t)
	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()
	require.NoError(t, setup.StartServer())
	require.NoError(t, setup.WaitUntilServerIsBootstrapped())

	// Make sure that the server's internal clock function returns pacific timezone
	start := setup.NowFn()().ToTime()
	setup.SetNowFn(xtime.ToUnixNano(start.In(pacificLocation)))

	// Instantiate a client
	client := setup.M3DBClient()
	session, err := client.DefaultSession()
	require.NoError(t, err)
	defer session.Close()

	// Generate test datapoints (all with NY timezone)
	namespace := opts.Namespaces()[0].ID().String()
	startNy := xtime.ToUnixNano(start.In(nyLocation))
	writeSeries := []readWriteTZCase{
		readWriteTZCase{
			namespace: namespace,
			id:        "some-id-1",
			datapoints: []readWriteTZDP{
				readWriteTZDP{
					value:     20.0,
					timestamp: startNy,
				},
				readWriteTZDP{
					value:     20.0,
					timestamp: startNy.Add(1 * time.Second),
				},
				readWriteTZDP{
					value:     20.0,
					timestamp: startNy.Add(2 * time.Second),
				},
			},
		},
		readWriteTZCase{
			namespace: namespace,
			id:        "some-id-2",
			datapoints: []readWriteTZDP{
				readWriteTZDP{
					value:     30.0,
					timestamp: startNy,
				},
				readWriteTZDP{
					value:     30.0,
					timestamp: startNy.Add(1 * time.Second),
				},
				readWriteTZDP{
					value:     30.0,
					timestamp: startNy.Add(2 * time.Second),
				},
			},
		},
	}

	// Write datapoints
	for _, series := range writeSeries {
		for _, write := range series.datapoints {
			err = session.Write(ident.StringID(series.namespace), ident.StringID(series.id), write.timestamp, write.value, xtime.Second, nil)
			require.NoError(t, err)
		}
	}

	// Read datapoints back
	iters, err := session.FetchIDs(ident.StringID(namespace),
		ident.NewIDsIterator(ident.StringID("some-id-1"), ident.StringID("some-id-2")),
		startNy, startNy.Add(1*time.Hour))
	require.NoError(t, err)

	// Assert datapoints match what we wrote
	for i, iter := range iters.Iters() {
		for j := 0; iter.Next(); j++ {
			dp, _, _ := iter.Current()
			expectedDatapoint := writeSeries[i].datapoints[j]
			// Datapoints will comeback with the timezone set to the local timezone
			// of the machine that the client is running on. The Equal() method ensures
			// that the two time.Time struct's refer to the same instant in time
			require.True(t, expectedDatapoint.timestamp.Equal(dp.TimestampNanos))
			require.Equal(t, expectedDatapoint.value, dp.Value)
		}
	}
}
