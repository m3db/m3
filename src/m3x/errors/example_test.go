// Copyright (c) 2016 Uber Technologies, Inc.
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

package errors_test

import (
	"fmt"
	"strings"

	"github.com/m3db/m3x/errors"
)

func ExampleMultiError() {
	multiErr := errors.NewMultiError()

	for i := 0; i < 3; i++ {
		// Perform some work which may fail.
		err := fmt.Errorf("error %d", i)

		if err != nil {
			// Add returns a new MultiError.
			multiErr = multiErr.Add(err)
		}
	}

	if err := multiErr.FinalError(); err != nil {
		msg := strings.Replace(err.Error(), "\n", "; ", -1)
		fmt.Println(msg)
	}

	if err := multiErr.LastError(); err != nil {
		fmt.Println(err)
	}

	// Output:
	// error 0; error 1; error 2
	// error 2
}
