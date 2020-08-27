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

package storagemetadata

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestValidateMetricsType(t *testing.T) {
	assert.NoError(t, ValidateMetricsType(UnaggregatedMetricsType))
	assert.Error(t, ValidateMetricsType(MetricsType(math.MaxUint64)))
}

func TestMetricsTypeUnmarshalYAML(t *testing.T) {
	type config struct {
		Type MetricsType `yaml:"type"`
	}

	for _, value := range validMetricsTypes {
		str := fmt.Sprintf("type: %s\n", value.String())
		var cfg config
		require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))
		assert.Equal(t, value, cfg.Type)
	}

	var cfg config
	require.Error(t, yaml.Unmarshal([]byte("type: not_a_known_type\n"), &cfg))
}
