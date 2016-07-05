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

package client

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3db/encoding/tsz"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

type testValue struct {
	value      float64
	t          time.Time
	unit       xtime.Unit
	annotation []byte
}

func TestSessionFetchAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions().FetchBatchSize(2)
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	start := time.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	data := []struct {
		id     string
		values []testValue
	}{
		{"foo", []testValue{
			{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
			{2.0, start.Add(2 * time.Second), xtime.Second, nil},
			{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		}},
		{"bar", []testValue{
			{4.0, start.Add(1 * time.Second), xtime.Second, []byte{4, 5, 6}},
			{5.0, start.Add(2 * time.Second), xtime.Second, nil},
			{6.0, start.Add(3 * time.Second), xtime.Second, nil},
		}},
		{"baz", []testValue{
			{7.0, start.Add(1 * time.Minute), xtime.Second, []byte{7, 8, 9}},
			{8.0, start.Add(2 * time.Minute), xtime.Second, nil},
			{9.0, start.Add(3 * time.Minute), xtime.Second, nil},
		}},
	}

	var fetchOps []*fetchOp
	enqueueFn := func(idx int, op m3db.Op) {
		fetch, ok := op.(*fetchOp)
		assert.True(t, ok)
		// TODO: assert fetchop properties are correct
		fetchOps = append(fetchOps, fetch)
	}

	// We expect 2x fetch ops per host as host queue batch size two and there are three IDs
	enqueueFns := []testEnqueueFn{enqueueFn, enqueueFn}
	enqueueWg := mockHostQueues(ctrl, session, sessionTestReplicas, enqueueFns)

	// Callback with results when enqueued
	go func() {
		// TODO(r): work out why it takes so long to wait for enqueue to be called
		enqueueWg.Wait()

		for _, op := range fetchOps {
			for i, id := range op.request.Ids {
				calledCompletionFn := false
				for _, d := range data {
					if d.id != id {
						continue
					}

					// Options uses TSZ encoding by default
					encoder := tsz.NewEncoder(d.values[0].t, nil, nil)
					for _, value := range d.values {
						dp := m3db.Datapoint{
							Timestamp: value.t,
							Value:     value.value,
						}
						encoder.Encode(dp, value.unit, value.annotation)
					}
					seg := encoder.Stream().Segment()
					op.completionFns[i]([]*rpc.Segments{&rpc.Segments{
						Merged: &rpc.Segment{Head: seg.Head, Tail: seg.Tail},
					}}, nil)
					calledCompletionFn = true
					break
				}
				assert.True(t, calledCompletionFn, "must call completion for ID", id)
			}
		}
	}()

	assert.NoError(t, session.Open())

	ids := []string{"foo", "bar", "baz"}
	results, err := session.FetchAll(ids, start, end)
	assert.NoError(t, err)
	assert.Equal(t, len(data), len(results))

	for i, series := range results {
		expected := data[i]
		assert.Equal(t, expected.id, series.ID())
		assert.Equal(t, start, series.Start())
		assert.Equal(t, end, series.End())

		j := 0
		fmt.Printf("checking %s\n", expected.id)
		for series.Next() {
			value := expected.values[j]
			dp, unit, annotation := series.Current()
			fmt.Printf("expected %v, %v\n", value.t, value.value)
			fmt.Printf("actual %v, %v\n", dp.Timestamp, dp.Value)
			assert.True(t, dp.Timestamp.Equal(value.t))
			assert.Equal(t, value.value, dp.Value)
			assert.Equal(t, value.unit, unit)
			assert.Equal(t, value.annotation, []byte(annotation))
			j++
		}
		assert.Equal(t, len(expected.values), j)
		assert.NoError(t, series.Err())
	}
	results.CloseAll()

	assert.NoError(t, session.Close())
}
