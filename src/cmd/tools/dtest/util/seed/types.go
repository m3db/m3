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

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
)

// Generator generates data
type Generator interface {
	Generate(namespace ident.ID, shard uint32) error
}

// Options control the knobs to generate data
type Options interface {

	// SetInstrumentOptions sets the instrument Options
	SetInstrumentOptions(instrument.Options) Options

	// InstrumentOptions returns the instrument Options
	InstrumentOptions() instrument.Options

	// SetGenerateOptions sets the generation.Options
	SetGenerateOptions(generate.Options) Options

	// GenerateOptions returns the generation.Options
	GenerateOptions() generate.Options

	// SetRandSource sets the Source used during generation
	SetRandSource(rand.Source) Options

	// RandSource returns the Source used during generation
	RandSource() rand.Source

	// SetNumIDs sets the number of ids generated per block
	SetNumIDs(int) Options

	// NumIDs returns the number of ids generated per block
	NumIDs() int

	// SetMinNumPointsPerID sets the min number of points
	// generated per ID per block
	SetMinNumPointsPerID(int) Options

	// MinNumPointsPerID returns the min number of points
	// generated per ID per block
	MinNumPointsPerID() int

	// SetMaxNumPointsPerID sets the max number of points
	// generated per ID per block
	SetMaxNumPointsPerID(int) Options

	// MaxNumPointsPerID returns the max number of points
	// generated per ID per block
	MaxNumPointsPerID() int

	// SetIDLengthMean sets the mean length used during ID generation
	SetIDLengthMean(float64) Options

	// IDLengthMean returns the mean length used during ID generation
	IDLengthMean() float64

	// SetIDLengthStddev sets the stddev of length used during ID generation
	SetIDLengthStddev(float64) Options

	// IDLengthStddev returns the stddev of length used during ID generation
	IDLengthStddev() float64
}
