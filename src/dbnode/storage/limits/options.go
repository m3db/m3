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

package limits

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/x/instrument"
)

type limitOpts struct {
	iOpts                   instrument.Options
	docsLimitOpts           LookbackLimitOptions
	bytesReadLimitOpts      LookbackLimitOptions
	diskSeriesReadLimitOpts LookbackLimitOptions
	sourceLoggerBuilder     SourceLoggerBuilder
}

// NewOptions creates limit options with default values.
func NewOptions() Options {
	return &limitOpts{
		sourceLoggerBuilder: &sourceLoggerBuilder{},
	}
}

// Validate validates the options.
func (o *limitOpts) Validate() error {
	if o.iOpts == nil {
		return errors.New("limit options invalid: no instrument options")
	}

	if err := o.docsLimitOpts.validate(); err != nil {
		return fmt.Errorf("doc limit options invalid: %w", err)
	}

	if err := o.bytesReadLimitOpts.validate(); err != nil {
		return fmt.Errorf("bytes limit options invalid: %w", err)
	}

	return nil
}

// SetInstrumentOptions sets the instrument options.
func (o *limitOpts) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.iOpts = value
	return &opts
}

// InstrumentOptions returns the instrument options.
func (o *limitOpts) InstrumentOptions() instrument.Options {
	return o.iOpts
}

// SetDocsLimitOpts sets the doc limit options.
func (o *limitOpts) SetDocsLimitOpts(value LookbackLimitOptions) Options {
	opts := *o
	opts.docsLimitOpts = value
	return &opts
}

// DocsLimitOpts returns the doc limit options.
func (o *limitOpts) DocsLimitOpts() LookbackLimitOptions {
	return o.docsLimitOpts
}

// SetBytesReadLimitOpts sets the byte read limit options.
func (o *limitOpts) SetBytesReadLimitOpts(value LookbackLimitOptions) Options {
	opts := *o
	opts.bytesReadLimitOpts = value
	return &opts
}

// BytesReadLimitOpts returns the byte read limit options.
func (o *limitOpts) BytesReadLimitOpts() LookbackLimitOptions {
	return o.bytesReadLimitOpts
}

// SetDiskSeriesReadLimitOpts sets the disk ts read limit options.
func (o *limitOpts) SetDiskSeriesReadLimitOpts(value LookbackLimitOptions) Options {
	opts := *o
	opts.diskSeriesReadLimitOpts = value
	return &opts
}

// DiskSeriesReadLimitOpts returns the disk ts read limit options.
func (o *limitOpts) DiskSeriesReadLimitOpts() LookbackLimitOptions {
	return o.diskSeriesReadLimitOpts
}

// SetSourceLoggerBuilder sets the source logger.
func (o *limitOpts) SetSourceLoggerBuilder(value SourceLoggerBuilder) Options {
	opts := *o
	opts.sourceLoggerBuilder = value
	return &opts
}

// SourceLoggerBuilder sets the source logger.
func (o *limitOpts) SourceLoggerBuilder() SourceLoggerBuilder {
	return o.sourceLoggerBuilder
}
