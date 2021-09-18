// Copyright (c) 2020 Uber Technologies, Inc.
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

package migration

import (
	"fmt"
	"math"
	"runtime"
)

// defaultMigrationConcurrency is the default number of concurrent workers to perform migrations.
var defaultMigrationConcurrency = int(math.Ceil(float64(runtime.GOMAXPROCS(0)) / 2))

type options struct {
	targetMigrationVersion MigrationVersion
	concurrency            int
}

// NewOptions creates new migration options.
func NewOptions() Options {
	return &options{
		concurrency: defaultMigrationConcurrency,
	}
}

func (o *options) Validate() error {
	if err := ValidateMigrationVersion(o.targetMigrationVersion); err != nil {
		return err
	}
	if o.concurrency < 1 {
		return fmt.Errorf("concurrency value %d must be >= 1", o.concurrency)
	}
	return nil
}

func (o *options) SetTargetMigrationVersion(value MigrationVersion) Options {
	opts := *o
	opts.targetMigrationVersion = value
	return &opts
}

func (o *options) TargetMigrationVersion() MigrationVersion {
	return o.targetMigrationVersion
}

func (o *options) SetConcurrency(value int) Options {
	opts := *o
	opts.concurrency = value
	return &opts
}

func (o *options) Concurrency() int {
	return o.concurrency
}
