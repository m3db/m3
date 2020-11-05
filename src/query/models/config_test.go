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

package models

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestIDSchemeValidation(t *testing.T) {
	err := TypeDefault.Validate()
	assert.EqualError(t, err, "id scheme type not set")
	err = TypePrependMeta.Validate()
	assert.NoError(t, err)
	err = TypeQuoted.Validate()
	assert.NoError(t, err)
	err = TypeGraphite.Validate()
	assert.NoError(t, err)
	err = IDSchemeType(5).Validate()
	assert.EqualError(t, err, "invalid config id schema type 'unknown':"+
		" should be one of [quoted prepend_meta graphite]")
}

func TestMetricsTypeUnmarshalYAML(t *testing.T) {
	type config struct {
		Type IDSchemeType `yaml:"type"`
	}

	validParseSchemes := []IDSchemeType{
		TypeQuoted,
		TypePrependMeta,
	}

	for _, value := range validParseSchemes {
		str := fmt.Sprintf("type: %s\n", value.String())

		var cfg config
		require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))

		assert.Equal(t, value, cfg.Type)
	}

	var cfg config
	// Graphite fails.
	require.Error(t, yaml.Unmarshal([]byte("type: graphite\n"), &cfg))
	// Bad type fails.
	require.Error(t, yaml.Unmarshal([]byte("type: not_a_known_type\n"), &cfg))

	require.NoError(t, yaml.Unmarshal([]byte(""), &cfg))
	assert.Equal(t, TypeDefault, cfg.Type)
}
