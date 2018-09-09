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

package encoding

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestValidIterateEqualTimestampStrategies(t *testing.T) {
	result := ValidIterateEqualTimestampStrategies()
	p1 := uintptr(unsafe.Pointer(&validIterateEqualTimestampStrategies))
	p2 := uintptr(unsafe.Pointer(&result))
	assert.NotEqual(t, p1, p2)
}

func TestIterateEqualTimestampStrategyUnmarshalYAML(t *testing.T) {
	type config struct {
		Strategy IterateEqualTimestampStrategy `yaml:"strategy"`
	}

	for _, value := range ValidIterateEqualTimestampStrategies() {
		str := fmt.Sprintf("strategy: %s\n", value.String())

		var cfg config
		require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))

		assert.Equal(t, value, cfg.Strategy)
	}

	var cfg config
	require.Error(t, yaml.Unmarshal([]byte("strategy: not_a_known_type\n"), &cfg))
}
