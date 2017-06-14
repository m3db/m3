package seed

import (
	"math/rand"

	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/instrument"
)

// Generator generates data
type Generator interface {
	Generate(namespace ts.ID, shard uint32) error
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
