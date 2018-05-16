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

package config

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestProducerConfiguration(t *testing.T) {
	str := `
buffer:
  maxBufferSize: 100
writer:
  topicName: testTopic
`

	var cfg ProducerConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(nil, nil)
	cs.EXPECT().Services(gomock.Any()).Return(nil, nil)

	_, err := cfg.newOptions(cs, instrument.NewOptions())
	require.NoError(t, err)
}
