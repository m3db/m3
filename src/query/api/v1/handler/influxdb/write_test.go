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

package influxdb

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	imodels "github.com/influxdata/influxdb/models"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/models"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// human-readable string out of what the iterator produces;
// they are easiest for human to handle
func (self *ingestIterator) pop(t *testing.T) string {
	if self.Next() {
		value := self.Current()
		assert.Equal(t, 1, len(value.Datapoints))

		return fmt.Sprintf("%s %v %d", value.Tags.String(), value.Datapoints[0].Value, int64(value.Datapoints[0].Timestamp))
	}
	return ""
}

func TestIngestIterator(t *testing.T) {
	// test prometheus-illegal measure and label components (should be _s)
	// as well as all value types influxdb supports
	s := `?measure:!,?tag1:!=tval1,?tag2:!=tval2 ?key1:!=3,?key2:!=2i 1574838670386469800
?measure:!,?tag1:!=tval1,?tag2:!=tval2 ?key3:!="string",?key4:!=T 1574838670386469801
`
	points, err := imodels.ParsePoints([]byte(s))
	require.NoError(t, err)
	iter := &ingestIterator{points: points, promRewriter: newPromRewriter()}
	require.NoError(t, iter.Error())
	for _, line := range []string{
		"__name__: _measure:___key1:_, _tag1__: tval1, _tag2__: tval2 3 1574838670386469800",
		"__name__: _measure:___key2:_, _tag1__: tval1, _tag2__: tval2 2 1574838670386469800",
		"__name__: _measure:___key4:_, _tag1__: tval1, _tag2__: tval2 1 1574838670386469801",
		"",
		"",
	} {
		assert.Equal(t, line, iter.pop(t))
	}
	require.NoError(t, iter.Error())
}

func TestIngestIteratorDuplicateTag(t *testing.T) {
	// Ensure that duplicate tag causes error and no metrics entries
	s := `measure,lab!=2,lab?=3 key=2i 1574838670386469800
`
	points, err := imodels.ParsePoints([]byte(s))
	require.NoError(t, err)
	iter := &ingestIterator{points: points, promRewriter: newPromRewriter()}
	require.NoError(t, iter.Error())
	for _, line := range []string{
		"",
	} {
		assert.Equal(t, line, iter.pop(t))
	}
	require.EqualError(t, iter.Error(), "non-unique Prometheus label lab_")
}

func TestIngestIteratorDuplicateNameTag(t *testing.T) {
	// Ensure that duplicate name tag causes error and no metrics entries
	s := `measure,__name__=x key=2i 1574838670386469800
`
	points, err := imodels.ParsePoints([]byte(s))
	require.NoError(t, err)
	iter := &ingestIterator{points: points, promRewriter: newPromRewriter()}
	require.NoError(t, iter.Error())
	for _, line := range []string{
		"",
	} {
		assert.Equal(t, line, iter.pop(t))
	}
	require.EqualError(t, iter.Error(), "non-unique Prometheus label __name__")
}

func TestIngestIteratorIssue2125(t *testing.T) {
	// In the issue, the Tags object is reused across Next()+Current() calls
	s := `measure,lab=foo k1=1,k2=2 1574838670386469800
`
	points, err := imodels.ParsePoints([]byte(s))
	require.NoError(t, err)

	iter := &ingestIterator{points: points, promRewriter: newPromRewriter()}
	require.NoError(t, iter.Error())

	assert.True(t, iter.Next())
	value1 := iter.Current()

	assert.True(t, iter.Next())
	value2 := iter.Current()
	require.NoError(t, iter.Error())

	assert.Equal(t, value1.Tags.String(), "__name__: measure_k1, lab: foo")
	assert.Equal(t, value2.Tags.String(), "__name__: measure_k2, lab: foo")
}

func TestIngestIteratorWriteTags(t *testing.T) {
	s := `measure,lab=foo k1=1,k2=2 1574838670386469800
`
	points, err := imodels.ParsePoints([]byte(s))
	require.NoError(t, err)

	writeTags := models.EmptyTags().
		AddTag(models.Tag{Name: []byte("lab"), Value: []byte("bar")}).
		AddTag(models.Tag{Name: []byte("new"), Value: []byte("tag")})

	iter := &ingestIterator{points: points, promRewriter: newPromRewriter(), writeTags: writeTags}

	assert.True(t, iter.Next())
	value1 := iter.Current()
	require.NoError(t, iter.Error())

	assert.Equal(t, value1.Tags.String(), "__name__: measure_k1, lab: bar, new: tag")

	assert.True(t, iter.Next())
	value2 := iter.Current()
	require.NoError(t, iter.Error())

	assert.Equal(t, value2.Tags.String(), "__name__: measure_k2, lab: bar, new: tag")
}

func TestDetermineTimeUnit(t *testing.T) {
	now := time.Now()
	zerot := now.Add(time.Duration(-now.UnixNano() % int64(time.Second)))
	assert.Equal(t, determineTimeUnit(zerot.Add(1*time.Second)), xtime.Second)
	assert.Equal(t, determineTimeUnit(zerot.Add(2*time.Millisecond)), xtime.Millisecond)
	assert.Equal(t, determineTimeUnit(zerot.Add(3*time.Microsecond)), xtime.Microsecond)
	assert.Equal(t, determineTimeUnit(zerot.Add(4*time.Nanosecond)), xtime.Nanosecond)
}

func makeOptions(ds ingest.DownsamplerAndWriter) options.HandlerOptions {
	return options.EmptyHandlerOptions().
		SetDownsamplerAndWriter(ds)
}

func makeInfluxDBLineProtocolMessage(t *testing.T, isGzipped bool, time time.Time, precision time.Duration) io.Reader {
	t.Helper()
	ts := fmt.Sprintf("%d", time.UnixNano()/precision.Nanoseconds())
	line := fmt.Sprintf("weather,location=us-midwest,season=summer temperature=82 %s", ts)
	var msg bytes.Buffer
	if isGzipped {
		gz := gzip.NewWriter(&msg)
		_, err := gz.Write([]byte(line))
		require.NoError(t, err)
		err = gz.Close()
		require.NoError(t, err)
	} else {
		msg.WriteString(line)
	}
	return bytes.NewReader(msg.Bytes())
}

func TestInfluxDBWrite(t *testing.T) {
	type checkWriteBatchFunc func(context.Context, *ingestIterator, ingest.WriteOptions) interface{}

	// small helper for tests where we dont want to check the batch
	dontCheckWriteBatch := checkWriteBatchFunc(
		func(context.Context, *ingestIterator, ingest.WriteOptions) interface{} {
			return nil
		},
	)

	tests := []struct {
		name            string
		expectedStatus  int
		requestHeaders  map[string]string
		isGzipped       bool
		checkWriteBatch checkWriteBatchFunc
	}{
		{
			name:           "Gzip Encoded Message",
			expectedStatus: http.StatusNoContent,
			isGzipped:      true,
			requestHeaders: map[string]string{
				"Content-Encoding": "gzip",
			},
			checkWriteBatch: dontCheckWriteBatch,
		},
		{
			name:           "Wrong Content Encoding",
			expectedStatus: http.StatusBadRequest,
			isGzipped:      false,
			requestHeaders: map[string]string{
				"Content-Encoding": "gzip",
			},
			checkWriteBatch: dontCheckWriteBatch,
		},
		{
			name:            "Plaintext Message",
			expectedStatus:  http.StatusNoContent,
			isGzipped:       false,
			requestHeaders:  map[string]string{},
			checkWriteBatch: dontCheckWriteBatch,
		},
		{
			name:           "Map-Tags-JSON Add Tag",
			expectedStatus: http.StatusNoContent,
			isGzipped:      false,
			requestHeaders: map[string]string{
				"M3-Map-Tags-JSON": `{"tagMappers": [{"write": {"tag": "t", "value": "v"}}]}`,
			},
			checkWriteBatch: checkWriteBatchFunc(
				func(_ context.Context, iter *ingestIterator, opts ingest.WriteOptions) interface{} {
					_, found := iter.writeTags.Get([]byte("t"))
					require.True(t, found, "tag t will be overwritten")
					return nil
				},
			),
		},
	}

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(tt *testing.T) {
			mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
			// For error reponses we don't expect WriteBatch to be called
			if testCase.expectedStatus != http.StatusBadRequest {
				mockDownsamplerAndWriter.
					EXPECT().
					WriteBatch(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(testCase.checkWriteBatch).
					Times(1)
			}

			opts := makeOptions(mockDownsamplerAndWriter)
			handler := NewInfluxWriterHandler(opts)
			msg := makeInfluxDBLineProtocolMessage(t, testCase.isGzipped, time.Now(), time.Nanosecond)
			req := httptest.NewRequest(InfluxWriteHTTPMethod, InfluxWriteURL, msg)
			for header, value := range testCase.requestHeaders {
				req.Header.Set(header, value)
			}
			writer := httptest.NewRecorder()
			handler.ServeHTTP(writer, req)
			resp := writer.Result()
			require.Equal(t, testCase.expectedStatus, resp.StatusCode)
			resp.Body.Close()
		})
	}
}

func TestInfluxDBWritePrecision(t *testing.T) {
	tests := []struct {
		name           string
		expectedStatus int
		precision      string
	}{
		{
			name:           "No precision",
			expectedStatus: http.StatusNoContent,
			precision:      "",
		},
		{
			name:           "Millisecond precision",
			expectedStatus: http.StatusNoContent,
			precision:      "ms",
		},
		{
			name:           "Second precision",
			expectedStatus: http.StatusNoContent,
			precision:      "s",
		},
	}

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(tt *testing.T) {
			var precision time.Duration
			switch testCase.precision {
			case "":
				precision = time.Nanosecond
			case "ms":
				precision = time.Millisecond
			case "s":
				precision = time.Second
			}

			now := time.Now()

			mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
			mockDownsamplerAndWriter.
				EXPECT().
				WriteBatch(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(
				_ context.Context,
				iter *ingestIterator,
				opts ingest.WriteOptions,
			) interface{} {
				require.Equal(tt, now.Truncate(precision).UnixNano(), iter.points[0].UnixNano(), "correct precision")
				return nil
			}).Times(1)

			opts := makeOptions(mockDownsamplerAndWriter)
			handler := NewInfluxWriterHandler(opts)

			msg := makeInfluxDBLineProtocolMessage(t, false, now, precision)
			var url string
			if testCase.precision == "" {
				url = InfluxWriteURL
			} else {
				url = InfluxWriteURL + fmt.Sprintf("?precision=%s", testCase.precision)
			}
			req := httptest.NewRequest(InfluxWriteHTTPMethod, url, msg)
			writer := httptest.NewRecorder()
			handler.ServeHTTP(writer, req)
			resp := writer.Result()
			require.Equal(t, testCase.expectedStatus, resp.StatusCode)
			resp.Body.Close()
		})
	}
}
