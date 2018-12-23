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

package downsample

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAsyncDownsamplerError(t *testing.T) {
	customErr := errors.New("some error")
	expectedErrStr := fmt.Sprintf(errNewDownsamplerFailFmt, customErr)

	done := make(chan struct{}, 1)
	asyncDownsampler := NewAsyncDownsampler(func() (Downsampler, error) {
		return nil, customErr
	}, done)
	require.NotNil(t, asyncDownsampler)

	// Wait for downsampler to be done initializing (which we mock to return an error)
	<-done

	_, err := asyncDownsampler.NewMetricsAppender()
	assert.EqualError(t, err, expectedErrStr)
}

func TestAsyncDownsamplerUninitialized(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDownsampler := NewMockDownsampler(ctrl)

	// Sleep until test has finished
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	asyncDownsampler := NewAsyncDownsampler(func() (Downsampler, error) {
		wg.Wait()
		return mockDownsampler, nil
	}, nil)
	require.NotNil(t, asyncDownsampler)

	_, err := asyncDownsampler.NewMetricsAppender()
	assert.EqualError(t, err, errDownsamplerUninitialized.Error())
}

func TestAsyncDownsamplerInitialized(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMetricsAppender := NewMockMetricsAppender(ctrl)

	mockDownsampler := NewMockDownsampler(ctrl)
	mockDownsampler.EXPECT().NewMetricsAppender().Return(mockMetricsAppender, nil)

	done := make(chan struct{}, 1)
	asyncDownsampler := NewAsyncDownsampler(func() (Downsampler, error) {
		return mockDownsampler, nil
	}, done)
	require.NotNil(t, asyncDownsampler)
	// Wait for session to be done initializing
	<-done

	metricsAppender, err := asyncDownsampler.NewMetricsAppender()
	assert.NoError(t, err)
	assert.True(t, metricsAppender == mockMetricsAppender)
}
