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

package seed

import (
	"math/rand"
	"time"

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3x/instrument"
)

const (
	defaulNumSeries       = 30000
	defaultMinPointsPerID = 50
	defaultMaxPointsPerID = 150
	defaultMeanIDLength   = 152.0
	defaultStddevIDLength = 66.0
)

type opts struct {
	generateOpts   generate.Options
	iOpts          instrument.Options
	source         rand.Source
	numIDs         int
	minPointsPerID int
	maxPointsPerID int
	idLenMean      float64
	idLenStddev    float64
}

// NewOptions returns new options
func NewOptions() Options {
	return &opts{
		generateOpts:   generate.NewOptions(),
		iOpts:          instrument.NewOptions(),
		source:         rand.NewSource(time.Now().UnixNano()),
		numIDs:         defaulNumSeries,
		minPointsPerID: defaultMinPointsPerID,
		maxPointsPerID: defaultMaxPointsPerID,
		idLenMean:      defaultMeanIDLength,
		idLenStddev:    defaultStddevIDLength,
	}
}

func (o *opts) SetInstrumentOptions(io instrument.Options) Options {
	o.iOpts = io
	return o
}

func (o *opts) InstrumentOptions() instrument.Options {
	return o.iOpts
}

func (o *opts) SetGenerateOptions(op generate.Options) Options {
	o.generateOpts = op
	return o
}

func (o *opts) GenerateOptions() generate.Options {
	return o.generateOpts
}

func (o *opts) SetRandSource(src rand.Source) Options {
	o.source = src
	return o
}

func (o *opts) RandSource() rand.Source {
	return o.source
}

func (o *opts) SetNumIDs(ni int) Options {
	o.numIDs = ni
	return o
}

func (o *opts) NumIDs() int {
	return o.numIDs
}

func (o *opts) SetMinNumPointsPerID(m int) Options {
	o.minPointsPerID = m
	return o
}

func (o *opts) MinNumPointsPerID() int {
	return o.minPointsPerID
}

func (o *opts) SetMaxNumPointsPerID(m int) Options {
	o.maxPointsPerID = m
	return o
}

func (o *opts) MaxNumPointsPerID() int {
	return o.maxPointsPerID
}

func (o *opts) SetIDLengthMean(l float64) Options {
	o.idLenMean = l
	return o
}

func (o *opts) IDLengthMean() float64 {
	return o.idLenMean
}

func (o *opts) SetIDLengthStddev(l float64) Options {
	o.idLenStddev = l
	return o
}

func (o *opts) IDLengthStddev() float64 {
	return o.idLenStddev
}
