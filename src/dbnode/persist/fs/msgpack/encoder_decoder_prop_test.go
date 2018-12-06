package msgpack

import (
	"fmt"
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

const minSuccessfulTests = 10000

func TestCommitlogFastEncodeDecodePropTest(t *testing.T) {
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

func genLogEntry() gopter.Gen {
	return gopter.CombineGens(
		gen.UInt64(),
		gen.Int64(),
		// gen.SliceOf(),
		gen.Int64(),
		gen.Float64(),
		gen.UInt32(),
	).Map(func(inputs []interface{}) schema.LogEntry {
		return schema.LogEntry{
			Index:  inputs[0].(uint64),
			Create: inputs[1].(int64),
			// TODO: Fix me
			Metadata:  nil,
			Timestamp: inputs[2].(int64),
			Value:     inputs[3].(float64),
			// TODO: Fix me
			Annotation: nil,
		}
	})
}
