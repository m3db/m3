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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryContext_WithContext(t *testing.T) {
	t.Run("passes along new context without modifying old", func(t *testing.T) {
		qc := NoopQueryContext()

		testKey := struct{}{}

		newCtx := context.WithValue(qc.Ctx, testKey, "bar")

		newQc := qc.WithContext(newCtx)

		assert.Equal(t, "bar", newQc.Ctx.Value(testKey), "new context should be present")
		assert.Equal(t, nil, qc.Ctx.Value(testKey), "old context should be the same")
	})

	t.Run("returns nil on nil", func(t *testing.T) {
		var qc *QueryContext
		assert.Nil(t, qc.WithContext(context.TODO()))
	})
}
