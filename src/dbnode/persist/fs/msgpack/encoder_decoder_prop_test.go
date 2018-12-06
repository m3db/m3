package msgpack

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3x/errors"
)

const minSuccessfulTests = 100000

func TestCommitlogFastEncodeDecodeLogEntryPropTest(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
	)

	parameters.MinSuccessfulTests = minSuccessfulTests
	parameters.Rng.Seed(seed)

	props.Property("Encodes and decodes successfully", prop.ForAll(func(input schema.LogEntry) (bool, error) {
		buf := []byte{}
		encoded, err := EncodeLogEntryFast(buf, input)
		if err != nil {
			return false, errors.Wrap(err, "error encoding log entry")
		}
		decoded, err := DecodeLogEntryFast(encoded)
		if err != nil {
			return false, errors.Wrap(err, "error decoding log entry")
		}

		if !reflect.DeepEqual(input, decoded) {
			return false, fmt.Errorf("expected: %v, but got: %v", input, decoded)
		}

		return true, nil
	}, genLogEntry()))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func TestCommitlogFastEncodeDecodeLogMetadataPropTest(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
	)

	parameters.MinSuccessfulTests = minSuccessfulTests
	parameters.Rng.Seed(seed)

	props.Property("Encodes and decodes successfully", prop.ForAll(func(input schema.LogMetadata) (bool, error) {
		buf := []byte{}
		encoded, err := EncodeLogMetadataFast(buf, input)
		if err != nil {
			return false, errors.Wrap(err, "error encoding log entry")
		}
		decoded, err := DecodeLogMetadataFast(encoded)
		if err != nil {
			return false, errors.Wrap(err, "error decoding log entry")
		}

		if !reflect.DeepEqual(input, decoded) {
			return false, fmt.Errorf("expected: %v, but got: %v", input, decoded)
		}

		return true, nil
	}, genLogMetadata()))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func TestCommitlogFastDecodeCorruptLogEntryPropTest(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
		rng        = rand.New(rand.NewSource(seed))
	)

	parameters.MinSuccessfulTests = minSuccessfulTests
	parameters.Rng.Seed(seed)

	props.Property("Encodes and decodes successfully with arbitrary corruption", prop.ForAll(func(input schema.LogEntry) (bool, error) {
		// Generate a valid byte stream.
		buf := []byte{}
		encoded, err := EncodeLogEntryFast(buf, input)
		if err != nil {
			return false, errors.Wrap(err, "error encoding log entry")
		}

		// Corrupt a single byte randomly and make sure it doesn't panic
		// when we try and decode.
		corruptIdx := rng.Intn(len(encoded))
		corruptVal := uint8(rng.Intn(math.MaxUint8))
		encoded[corruptIdx] = corruptVal

		// Ignore result and errors. We're just checking for panics. Errors are meaningless
		// because sometimes the corruption may generate an invalid msgpack encoding, and sometimes
		// it may still be decodable.
		DecodeLogEntryFast(encoded)

		return true, nil
	}, genLogEntry()))

	props.Property("Encodes and decodes successfully with arbitrary truncation", prop.ForAll(func(input schema.LogEntry) (bool, error) {
		// Generate a valid byte stream.
		buf := []byte{}
		encoded, err := EncodeLogEntryFast(buf, input)
		if err != nil {
			return false, errors.Wrap(err, "error encoding log entry")
		}

		// Pick an arbitrary spot to truncate the stream.
		corruptIdx := rng.Intn(len(encoded))
		encoded = encoded[:corruptIdx]

		// Ignore result and errors. We're just checking for panics. Errors are meaningless
		// because sometimes the corruption may generate an invalid msgpack encoding, and sometimes
		// it may still be decodable.
		DecodeLogEntryFast(encoded)

		return true, nil
	}, genLogEntry()))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func genLogEntry() gopter.Gen {
	return gopter.CombineGens(
		gen.UInt64(),
		gen.Int64(),
		genByteSlice(),
		gen.Int64(),
		gen.Float64(),
		gen.UInt32(),
		genByteSlice(),
	).Map(func(inputs []interface{}) schema.LogEntry {
		return schema.LogEntry{
			Index:      inputs[0].(uint64),
			Create:     inputs[1].(int64),
			Metadata:   inputs[2].([]byte),
			Timestamp:  inputs[3].(int64),
			Value:      inputs[4].(float64),
			Unit:       inputs[5].(uint32),
			Annotation: inputs[6].([]byte),
		}
	})
}

func genLogMetadata() gopter.Gen {
	return gopter.CombineGens(
		genByteSlice(),
		genByteSlice(),
		gen.UInt32(),
		genByteSlice(),
	).Map(func(inputs []interface{}) schema.LogMetadata {
		return schema.LogMetadata{
			ID:          inputs[0].([]byte),
			Namespace:   inputs[1].([]byte),
			Shard:       inputs[2].(uint32),
			EncodedTags: inputs[3].([]byte),
		}
	})
}

func genByteSlice() gopter.Gen {
	return gen.SliceOf(gen.UInt8())
}
