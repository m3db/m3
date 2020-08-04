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

package storage

import (
	"errors"
	"fmt"
	"testing"

	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestParseErrorBehavior(t *testing.T) {
	b, err := ParseErrorBehavior("fail")
	assert.NoError(t, err)
	assert.Equal(t, BehaviorFail, b)

	b, err = ParseErrorBehavior("warn")
	assert.NoError(t, err)
	assert.Equal(t, BehaviorWarn, b)

	_, err = ParseErrorBehavior(BehaviorContainer.String())
	assert.Error(t, err)

	_, err = ParseErrorBehavior("blah")
	assert.Error(t, err)

}

func TestErrorBehaviorUnmarshalYAML(t *testing.T) {
	type config struct {
		Type ErrorBehavior `yaml:"type"`
	}

	valid := []ErrorBehavior{BehaviorFail, BehaviorWarn}
	for _, value := range valid {
		str := fmt.Sprintf("type: %s\n", value.String())
		var cfg config
		require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))
		assert.Equal(t, value, cfg.Type)
	}

	container := fmt.Sprintf("type: %s\n", BehaviorContainer.String())
	var cfg config
	require.Error(t, yaml.Unmarshal([]byte(container), &cfg))
	require.Error(t, yaml.Unmarshal([]byte("type: not_a_known_type\n"), &cfg))
}

func TestWarnError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	store := NewMockStorage(ctrl)

	err := errors.New("e")
	warnErr := warnError{inner: errors.New("f")}

	// NB: this shouldn't go to store.
	warn, wrapped := IsWarning(store, warnErr)
	assert.True(t, warn)
	_, ok := wrapped.(warnError)
	assert.True(t, ok)

	store.EXPECT().ErrorBehavior().Return(BehaviorFail).Times(1)
	warn, wrapped = IsWarning(store, err)
	assert.False(t, warn)
	_, ok = wrapped.(warnError)
	assert.False(t, ok)

	store.EXPECT().ErrorBehavior().Return(BehaviorWarn).Times(1)
	warn, wrapped = IsWarning(store, err)
	assert.True(t, warn)
	_, ok = wrapped.(warnError)
	assert.True(t, ok)
}
