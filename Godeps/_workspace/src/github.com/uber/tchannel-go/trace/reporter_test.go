// Copyright (c) 2015 Uber Technologies, Inc.

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

package trace

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/json"
	"github.com/uber/tchannel-go/testutils"
	"github.com/uber/tchannel-go/thrift"
	gen "github.com/uber/tchannel-go/trace/thrift/gen-go/tcollector"
	"github.com/uber/tchannel-go/trace/thrift/mocks"
)

func TestTraceReporterFactory(t *testing.T) {
	opts := testutils.NewOpts().
		SetServiceName("svc").
		SetTraceReporterFactory(TCollectorReporterFactory)
	ch := testutils.NewClient(t, opts)

	// Create a TCollector channel, and add it as a peer to ch so Report works.
	mockServer := new(mocks.TChanTCollector)
	tcollectorCh := setupServer(t, mockServer)
	ch.Peers().Add(tcollectorCh.PeerInfo().HostPort)

	called := make(chan int)
	ret := &gen.Response{Ok: true}
	mockServer.On("Submit", mock.Anything, mock.Anything).Return(ret, nil).Run(func(args mock.Arguments) {
		called <- 1
	})

	// Make some calls, and validate that the trace reporter is not called recursively.
	require.NoError(t, json.Register(tcollectorCh, json.Handlers{"op": func(ctx json.Context, arg map[string]interface{}) (map[string]interface{}, error) {
		return arg, nil
	}}, nil), "Register failed")
	ctx, cancel := json.NewContext(time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		var res map[string]string
		assert.NoError(t, json.CallSC(ctx, ch.GetSubChannel(tcollectorServiceName), "op", nil, &res), "call failed")
	}

	// Verify the spans being reported.
	for i := 0; i < 5; i++ {
		select {
		case <-called:
		case <-time.After(time.Second):
			t.Errorf("Expected submit for call %v", i)
		}
	}

	// Verify that no other spans are reported.
	select {
	case <-called:
		t.Errorf("Too many spans reported")
	case <-time.After(time.Millisecond):
	}
}

func TestBuildSpan(t *testing.T) {
	data := &tchannel.TraceData{
		Span:              *tchannel.NewRootSpan(),
		BinaryAnnotations: []tchannel.BinaryAnnotation{{Key: "cn", Value: "string"}},
		Source: tchannel.TraceEndpoint{
			HostPort:    "0.0.0.0:0",
			ServiceName: "testServer",
		},
		Target: tchannel.TraceEndpoint{
			HostPort:    "127.0.0.1:9999",
			ServiceName: "targetServer",
		},
		Method: "test",
	}
	_, data.Annotations = RandomAnnotations()

	thriftSpan, err := buildSpan(data, 12345)
	assert.NoError(t, err)
	binaryAnns, err := buildBinaryAnnotations(data.BinaryAnnotations)
	assert.NoError(t, err)
	expectedSpan := &gen.Span{
		TraceId: uint64ToBytes(data.Span.TraceID()),
		SpanHost: &gen.Endpoint{
			Ipv4:        12345,
			Port:        0,
			ServiceName: "testServer",
		},
		Host: &gen.Endpoint{
			Ipv4:        (int32)(inetAton("127.0.0.1")),
			Port:        9999,
			ServiceName: "targetServer",
		},
		Name:              "test",
		ID:                uint64ToBytes(data.Span.SpanID()),
		ParentId:          uint64ToBytes(data.Span.ParentID()),
		Annotations:       buildAnnotations(data.Annotations),
		BinaryAnnotations: binaryAnns,
	}

	assert.Equal(t, thriftSpan, expectedSpan, "Span mismatch")
}

func TestInetAton(t *testing.T) {
	assert.Equal(t, inetAton("1.2.3.4"), uint32(16909060))
}

func TestUInt64ToBytes(t *testing.T) {
	assert.Equal(t, uint64ToBytes(54613478251749257), []byte("\x00\xc2\x06\xabK$\xdf\x89"))
}

func TestBase64Encode(t *testing.T) {
	assert.Equal(t, base64Encode(12711515087145684), "AC0pDj1TitQ=")
}

func TestBuildAnnotations(t *testing.T) {
	baseTime, testAnnotations := RandomAnnotations()
	baseTimeMillis := float64(1420167845000)
	testExpected := []*gen.Annotation{
		{
			Timestamp: baseTimeMillis + 1000,
			Value:     "cr",
		},
		{
			Timestamp: baseTimeMillis + 2000.0,
			Value:     "cs",
		},
		{
			Timestamp: baseTimeMillis + 3000,
			Value:     "sr",
		},
		{
			Timestamp: baseTimeMillis + 4000,
			Value:     "ss",
		},
	}

	makeTCAnnotations := func(ts time.Time) []tchannel.Annotation {
		return []tchannel.Annotation{{
			Key:       tchannel.AnnotationKeyClientReceive,
			Timestamp: ts,
		}}
	}
	makeGenAnnotations := func(ts float64) []*gen.Annotation {
		return []*gen.Annotation{{
			Value:     "cr",
			Timestamp: ts,
		}}
	}

	tests := []struct {
		annotations []tchannel.Annotation
		expected    []*gen.Annotation
	}{
		{
			annotations: nil,
			expected:    []*gen.Annotation{},
		},
		{
			annotations: makeTCAnnotations(baseTime.Add(time.Nanosecond)),
			expected:    makeGenAnnotations(baseTimeMillis),
		},
		{
			annotations: makeTCAnnotations(baseTime.Add(time.Microsecond)),
			expected:    makeGenAnnotations(baseTimeMillis),
		},
		{
			annotations: makeTCAnnotations(baseTime.Add(time.Millisecond)),
			expected:    makeGenAnnotations(baseTimeMillis + 1),
		},
		{
			annotations: testAnnotations,
			expected:    testExpected,
		},
	}

	for _, tt := range tests {
		got := buildAnnotations(tt.annotations)
		assert.Equal(t, tt.expected, got, "result spans mismatch")
	}
}

func RandomAnnotations() (time.Time, []tchannel.Annotation) {
	baseTime := time.Date(2015, 1, 2, 3, 4, 5, 6, time.UTC)
	return baseTime, []tchannel.Annotation{
		{
			Key:       tchannel.AnnotationKeyClientReceive,
			Timestamp: baseTime.Add(time.Second),
		},
		{
			Key:       tchannel.AnnotationKeyClientSend,
			Timestamp: baseTime.Add(2 * time.Second),
		},
		{
			Key:       tchannel.AnnotationKeyServerReceive,
			Timestamp: baseTime.Add(3 * time.Second),
		},
		{
			Key:       tchannel.AnnotationKeyServerSend,
			Timestamp: baseTime.Add(4 * time.Second),
		},
	}
}

type testArgs struct {
	s *mocks.TChanTCollector
	c tchannel.TraceReporter
}

func ctxArg() mock.AnythingOfTypeArgument {
	return mock.AnythingOfType("tchannel.headerCtx")
}

func TestSubmit(t *testing.T) {
	withSetup(t, func(ctx thrift.Context, args testArgs) {
		data, expected := submitArgs(t)
		called := make(chan struct{})
		ret := &gen.Response{Ok: true}
		args.s.On("Submit", ctxArg(), expected).Return(ret, nil).Run(func(_ mock.Arguments) {
			close(called)
		})
		args.c.Report(*data)

		// wait for the server's Submit to get called
		select {
		case <-time.After(time.Second):
			t.Fatal("Submit not called")
		case <-called:
		}
	})
}

func submitArgs(t testing.TB) (*tchannel.TraceData, *gen.Span) {
	data := &tchannel.TraceData{
		Span:              *tchannel.NewRootSpan(),
		BinaryAnnotations: RandomBinaryAnnotations(),
		Method:            "echo",
		Source: tchannel.TraceEndpoint{
			HostPort:    "127.0.0.1:8888",
			ServiceName: "testServer",
		},
		Target: tchannel.TraceEndpoint{
			HostPort:    "127.0.0.1:9999",
			ServiceName: "targetServer",
		},
	}
	_, data.Annotations = RandomAnnotations()

	genSpan, err := buildSpan(data, 0)
	require.NoError(t, err, "Build test span failed")

	return data, genSpan
}

func TestSubmitNotRetried(t *testing.T) {
	withSetup(t, func(ctx thrift.Context, args testArgs) {
		data, expected := submitArgs(t)
		count := 0
		args.s.On("Submit", ctxArg(), expected).Return(nil, tchannel.ErrServerBusy).Run(func(_ mock.Arguments) {
			count++
		})
		args.c.Report(*data)

		// Report another span that we use to detect that the previous span has been processed.
		data, expected = submitArgs(t)
		completed := make(chan struct{})
		args.s.On("Submit", ctxArg(), expected).Return(nil, nil).Run(func(_ mock.Arguments) {
			close(completed)
		})
		args.c.Report(*data)

		// wait for the server's Submit to get called
		select {
		case <-time.After(time.Second):
			t.Fatal("Submit not called")
		case <-completed:
		}

		// Verify that the initial span was not retried multiple times.
		assert.Equal(t, 1, count, "tcollector Submit should not be retried")
	})
}

func withSetup(t *testing.T, f func(ctx thrift.Context, args testArgs)) {
	args := testArgs{
		s: new(mocks.TChanTCollector),
	}

	ctx, cancel := thrift.NewContext(time.Second * 10)
	defer cancel()

	// Start server
	tchan := setupServer(t, args.s)
	defer tchan.Close()

	// Get client1
	args.c = getClient(t, tchan.PeerInfo().HostPort)

	f(ctx, args)

	args.s.AssertExpectations(t)
}

func setupServer(t *testing.T, h *mocks.TChanTCollector) *tchannel.Channel {
	tchan := testutils.NewServer(t, testutils.NewOpts().SetServiceName(tcollectorServiceName))
	server := thrift.NewServer(tchan)
	server.Register(gen.NewTChanTCollectorServer(h))
	return tchan
}

func getClient(t *testing.T, dst string) tchannel.TraceReporter {
	tchan := testutils.NewClient(t, nil)
	tchan.Peers().Add(dst)
	return NewTCollectorReporter(tchan)
}

func BenchmarkBuildThrift(b *testing.B) {
	data, _ := submitArgs(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildSpan(data, 0)
	}
}

type BinaryAnnotationTestArgs struct {
	annotation tchannel.BinaryAnnotation
	expected   *gen.BinaryAnnotation
}

func generateBinaryAnnotationsTestCase() []BinaryAnnotationTestArgs {
	s := "testString"
	ii64 := int64(5)
	_ii64 := int64(5)
	ii32 := int32(6)
	_ii32 := int64(6)
	ii16 := int16(7)
	_ii16 := int64(7)
	i := 8
	_i := int64(8)
	b := false
	f32 := float32(5.0)
	_f32 := float64(5.0)
	f64 := float64(6.0)
	_f64 := float64(6.0)
	bs := []byte{4, 3, 2}
	return []BinaryAnnotationTestArgs{
		{
			annotation: tchannel.BinaryAnnotation{Key: "string", Value: s},
			expected:   &gen.BinaryAnnotation{Key: "string", StringValue: &s, AnnotationType: gen.AnnotationType_STRING},
		},
		{
			annotation: tchannel.BinaryAnnotation{Key: "int", Value: i},
			expected:   &gen.BinaryAnnotation{Key: "int", IntValue: &_i, AnnotationType: gen.AnnotationType_I32},
		},
		{
			annotation: tchannel.BinaryAnnotation{Key: "int16", Value: ii16},
			expected:   &gen.BinaryAnnotation{Key: "int16", IntValue: &_ii16, AnnotationType: gen.AnnotationType_I16},
		},
		{
			annotation: tchannel.BinaryAnnotation{Key: "int32", Value: ii32},
			expected:   &gen.BinaryAnnotation{Key: "int32", IntValue: &_ii32, AnnotationType: gen.AnnotationType_I32},
		},
		{
			annotation: tchannel.BinaryAnnotation{Key: "int64", Value: ii64},
			expected:   &gen.BinaryAnnotation{Key: "int64", IntValue: &_ii64, AnnotationType: gen.AnnotationType_I64},
		},
		{
			annotation: tchannel.BinaryAnnotation{Key: "bool", Value: b},
			expected:   &gen.BinaryAnnotation{Key: "bool", BoolValue: &b, AnnotationType: gen.AnnotationType_BOOL},
		},
		{
			annotation: tchannel.BinaryAnnotation{Key: "float32", Value: f32},
			expected:   &gen.BinaryAnnotation{Key: "float32", DoubleValue: &_f32, AnnotationType: gen.AnnotationType_DOUBLE},
		},
		{
			annotation: tchannel.BinaryAnnotation{Key: "float64", Value: f64},
			expected:   &gen.BinaryAnnotation{Key: "float64", DoubleValue: &_f64, AnnotationType: gen.AnnotationType_DOUBLE},
		},
		{
			annotation: tchannel.BinaryAnnotation{Key: "bytes", Value: bs},
			expected:   &gen.BinaryAnnotation{Key: "bytes", BytesValue: bs, AnnotationType: gen.AnnotationType_BYTES},
		},
	}
}

func RandomBinaryAnnotations() []tchannel.BinaryAnnotation {
	args := generateBinaryAnnotationsTestCase()
	bAnns := make([]tchannel.BinaryAnnotation, len(args))
	for i, arg := range args {
		bAnns[i] = arg.annotation
	}
	return bAnns
}

func TestBuildBinaryAnnotation(t *testing.T) {
	tests := generateBinaryAnnotationsTestCase()
	for _, tt := range tests {
		result, err := buildBinaryAnnotation(tt.annotation)
		assert.NoError(t, err, "Failed to build binary annotations.")
		assert.Equal(t, tt.expected, result, "BinaryAnnotation is mismatched.")
	}
}

func TestBuildBinaryAnnotationsWithEmptyList(t *testing.T) {
	result, err := buildBinaryAnnotations(nil)
	assert.NoError(t, err, "Failed to build binary annotations.")
	assert.Equal(t, len(result), 0, "BinaryAnnotations should be empty.")
}

func TestBuildBinaryAnnotationsWithMultiItems(t *testing.T) {
	tests := generateBinaryAnnotationsTestCase()
	var binaryAnns []tchannel.BinaryAnnotation
	var expectedAnns []*gen.BinaryAnnotation
	for _, tt := range tests {
		binaryAnns = append(binaryAnns, tt.annotation)
		expectedAnns = append(expectedAnns, tt.expected)
	}
	result, err := buildBinaryAnnotations(binaryAnns)
	assert.NoError(t, err, "Failed to build binary annotations.")
	assert.Equal(t, expectedAnns, result, "BinaryAnnotation is mismatched.")
}

func TestBuildBinaryAnnotationsWithError(t *testing.T) {
	_, err := buildBinaryAnnotations(
		[]tchannel.BinaryAnnotation{{Key: "app", Value: []bool{false}}},
	)
	assert.Error(t, err, "An Error was expected.")
}
