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

package annotated

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/iotest"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var testWriteRequest = prompb.AnnotatedWriteRequest{
	Timeseries: []prompb.AnnotatedTimeSeries{
		{
			Labels: []prompb.Label{
				{Name: []byte("__name__"), Value: []byte("requests")},
				{Name: []byte("status_code"), Value: []byte("200")},
			},
			Samples: []prompb.AnnotatedSample{
				{
					Value:      4.2,
					Timestamp:  testTimestampMillis,
					Annotation: []byte("foo"),
				},
				{
					Value:      3.14,
					Timestamp:  testTimestampMillis + 10000,
					Annotation: []byte("bar"),
				},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: []byte("__name__"), Value: []byte("requests")},
				{Name: []byte("status_code"), Value: []byte("500")},
			},
			Samples: []prompb.AnnotatedSample{
				{
					Value:      6.28,
					Timestamp:  testTimestampMillis,
					Annotation: []byte("baz"),
				},
			},
		},
	},
}

func TestHandlerServeHTTP(t *testing.T) {
	tests := []struct {
		name       string
		reqFn      func(t *testing.T) *http.Request
		handlerFn  func(ctrl *gomock.Controller) *Handler
		wantStatus int
	}{
		{
			name: "successful request",
			reqFn: func(t *testing.T) *http.Request {
				data, err := proto.Marshal(&testWriteRequest)
				require.NoError(t, err)

				data = snappy.Encode(nil, data)
				buf := bytes.NewBuffer(data)
				return httptest.NewRequest(WriteHTTPMethod, WriteURL, buf)
			},
			handlerFn: func(ctrl *gomock.Controller) *Handler {
				w := ingest.NewMockDownsamplerAndWriter(ctrl)
				w.EXPECT().WriteBatch(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				return NewHandler(w, models.NewTagOptions(), tally.NoopScope)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "internal error",
			reqFn: func(t *testing.T) *http.Request {
				data, err := proto.Marshal(&testWriteRequest)
				require.NoError(t, err)

				data = snappy.Encode(nil, data)
				buf := bytes.NewBuffer(data)
				return httptest.NewRequest(WriteHTTPMethod, WriteURL, buf)
			},
			handlerFn: func(ctrl *gomock.Controller) *Handler {
				var (
					w   = ingest.NewMockDownsamplerAndWriter(ctrl)
					err = xerrors.NewMultiError()
				)
				err = err.Add(errors.New("test error"))
				w.EXPECT().WriteBatch(gomock.Any(), gomock.Any(), gomock.Any()).Return(err)
				return NewHandler(w, models.NewTagOptions(), tally.NoopScope)
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "bad request",
			reqFn: func(t *testing.T) *http.Request {
				data, err := proto.Marshal(&testWriteRequest)
				require.NoError(t, err)

				data = snappy.Encode(nil, data)
				buf := bytes.NewBuffer(data)
				return httptest.NewRequest(WriteHTTPMethod, WriteURL, buf)
			},
			handlerFn: func(ctrl *gomock.Controller) *Handler {
				var (
					w   = ingest.NewMockDownsamplerAndWriter(ctrl)
					err = xerrors.NewMultiError()
				)
				err = err.Add(xerrors.NewInvalidParamsError(errors.New("invalid params")))
				w.EXPECT().WriteBatch(gomock.Any(), gomock.Any(), gomock.Any()).Return(err)
				return NewHandler(w, models.NewTagOptions(), tally.NoopScope)
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "empty body",
			reqFn: func(_ *testing.T) *http.Request {
				return httptest.NewRequest(WriteHTTPMethod, WriteURL, nil)
			},
			handlerFn: func(ctrl *gomock.Controller) *Handler {
				w := ingest.NewMockDownsamplerAndWriter(ctrl)
				return NewHandler(w, models.NewTagOptions(), tally.NoopScope)
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			var (
				h = tt.handlerFn(ctrl)
				r = tt.reqFn(t)
				w = httptest.NewRecorder()
			)
			h.ServeHTTP(w, r)

			resp := w.Result()
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}

func TestParseRequest(t *testing.T) {
	tests := []struct {
		name    string
		inputFn func(t *testing.T) io.Reader
		want    prompb.AnnotatedWriteRequest
		wantErr string
	}{
		{
			name: "valid write request",
			inputFn: func(t *testing.T) io.Reader {
				data, err := proto.Marshal(&testWriteRequest)
				require.NoError(t, err)

				data = snappy.Encode(nil, data)

				return bytes.NewBuffer(data)
			},
			want: testWriteRequest,
		},
		{
			name: "reader returns error",
			inputFn: func(t *testing.T) io.Reader {
				return iotest.TimeoutReader(bytes.NewBuffer([]byte("invalid")))
			},
			wantErr: "timeout",
		},
		{
			name: "invalid snappy payload",
			inputFn: func(_ *testing.T) io.Reader {
				return bytes.NewBuffer([]byte("invalid"))
			},
			wantErr: "unable to decode request body: snappy: corrupt input",
		},
		{
			name: "invalid write request",
			inputFn: func(t *testing.T) io.Reader {
				data := snappy.Encode(nil, []byte("invalid"))
				return bytes.NewBuffer(data)
			},
			wantErr: "unable to unmarshal request: unexpected EOF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := tt.inputFn(t)
			actual, err := parseRequest(input)
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, actual)
		})
	}
}
