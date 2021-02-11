// +build big
//
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

package proto

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"runtime/debug"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/builder"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

var (
	// Generated from mapProtoTypeToCustomFieldType by init().
	allowedProtoTypesSliceIface = []interface{}{}

	validMapKeyTypes = []interface{}{
		// https://developers.google.com/protocol-buffers/docs/proto3#maps
		dpb.FieldDescriptorProto_TYPE_INT32,
		dpb.FieldDescriptorProto_TYPE_INT64,
		dpb.FieldDescriptorProto_TYPE_UINT32,
		dpb.FieldDescriptorProto_TYPE_UINT64,
		dpb.FieldDescriptorProto_TYPE_SINT32,
		dpb.FieldDescriptorProto_TYPE_SINT64,
		dpb.FieldDescriptorProto_TYPE_FIXED32,
		dpb.FieldDescriptorProto_TYPE_FIXED64,
		dpb.FieldDescriptorProto_TYPE_SFIXED32,
		dpb.FieldDescriptorProto_TYPE_SFIXED64,
		dpb.FieldDescriptorProto_TYPE_BOOL,
		dpb.FieldDescriptorProto_TYPE_STRING,
	}
)

func init() {
	for key := range mapProtoTypeToCustomFieldType {
		allowedProtoTypesSliceIface = append(allowedProtoTypesSliceIface, key)
	}
}

const (
	maxNumFields     = 10
	maxNumMessages   = 100
	maxNumEnumValues = 10

	debugLogs = false
)

type fieldModifierProp int

const (
	fieldModifierRegular fieldModifierProp = iota
	fieldModifierReserved
	// Maps can't be repeated so its ok for these to be mutally exclusive.
	fieldModifierRepeated
	fieldModifierMap
)

func TestRoundTripProp(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
	)
	parameters.MinSuccessfulTests = 300
	parameters.Rng.Seed(seed)

	enc := NewEncoder(time.Time{}, testEncodingOptions)
	iter := NewIterator(nil, nil, testEncodingOptions).(*iterator)
	props.Property("Encoded data should be readable", prop.ForAll(func(input propTestInput) (bool, error) {
		if debugLogs {
			fmt.Println("---------------------------------------------------")
		}

		// Make panics debuggable.
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf(
					"recovered with err: %v,schema: %s and messages: %#v",
					r,
					input.schema.String(),
					input.messages)
				debug.PrintStack()
				panic("prop test encountered a panic!")
			}
		}()

		times := make([]time.Time, 0, len(input.messages))
		currTime := time.Now()
		for i, m := range input.messages {
			duration, err := m.timeUnit.Value()
			if err != nil {
				return false, fmt.Errorf("error getting duration from xtime.Unit: %v", err)
			}

			currTime = currTime.Add(time.Duration(i) * duration).Truncate(duration)
			times = append(times, currTime)
		}

		schemaDescr := namespace.GetTestSchemaDescr(input.schema)
		enc.Reset(currTime, 0, schemaDescr)

		for i, m := range input.messages {
			// The encoder will mutate the message so make sure we clone it first.
			clone := dynamic.NewMessage(input.schema)
			clone.MergeFrom(m.message)
			cloneBytes, err := clone.Marshal()
			if err != nil {
				return false, fmt.Errorf("error marshalling proto message: %v", err)
			}

			if debugLogs {
				printMessage(fmt.Sprintf("encoding %d", i), m.message)
			}
			err = enc.Encode(ts.Datapoint{Timestamp: times[i]}, xtime.Nanosecond, cloneBytes)
			if err != nil {
				return false, fmt.Errorf(
					"error encoding message: %v, schema: %s", err, input.schema.String())
			}

			// Ensure that the schema can be set inbetween writes without interfering with the stream.
			// A new deployID is created each time to bypass the quick check in the SetSchema method
			// and force the encoder to perform all of the state updates it has to do when a schema
			// changes but without actually changing the schema (which would make asserting on the
			// correct output difficult).
			setSchemaDescr := namespace.GetTestSchemaDescrWithDeployID(
				input.schema, fmt.Sprintf("deploy_id_%d", i))
			enc.SetSchema(setSchemaDescr)
		}

		// Ensure that the Len() method always returns the length of the final stream that would
		// be returned by a call to Stream().
		encLen := enc.Len()

		ctx := context.NewBackground()
		defer ctx.Close()

		stream, ok := enc.Stream(ctx)
		if !ok {
			if len(input.messages) == 0 {
				return true, nil
			}
			return false, fmt.Errorf("encoder returned empty stream")
		}
		segment, err := stream.Segment()
		if err != nil {
			return false, fmt.Errorf("error getting segment: %v", err)
		}
		if segment.Len() != encLen {
			return false, fmt.Errorf(
				"expected segment len (%d) to match encoder len (%d)",
				segment.Len(), encLen)
		}

		iter.Reset(stream, schemaDescr)

		i := 0
		for iter.Next() {
			var (
				m                    = input.messages[i].message
				dp, unit, annotation = iter.Current()
			)
			decodedM := dynamic.NewMessage(input.schema)
			require.NoError(t, decodedM.Unmarshal(annotation))
			if debugLogs {
				printMessage(fmt.Sprintf("decoding %d", i), decodedM)
			}

			require.Equal(t, unit, xtime.Nanosecond)
			require.True(t,
				times[i].Equal(dp.Timestamp),
				"%s does not match %s", times[i], dp.Timestamp)

			if !dynamic.MessagesEqual(m, decodedM) {
				for _, field := range m.GetKnownFields() {
					var (
						fieldNum    = int(field.GetNumber())
						expectedVal = m.GetFieldByNumber(fieldNum)
						actualVal   = decodedM.GetFieldByNumber(fieldNum)
					)

					if !fieldsEqual(expectedVal, actualVal) {
						return false, fmt.Errorf(
							"expected %v but got %v on iteration number %d and fieldNum %d, schema %s",
							expectedVal, actualVal, i, fieldNum, input.schema)
					}
				}
			}
			i++
		}

		if iter.Err() != nil {
			return false, fmt.Errorf(
				"iteration error: %v, schema: %s", iter.Err(), input.schema.String())
		}

		if i != len(input.messages) {
			return false, fmt.Errorf("expected %d messages but got %d", len(input.messages), i)
		}

		return true, nil
	}, genPropTestInputs()))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

// TestBijectivityProp ensures that the protobuf encoding format is bijective. I.E if the same messages are
// provided in the same order then the resulting stream should always be the same and similarly that decoding
// and re-encoding should always generate the same stream as the original.
func TestBijectivityProp(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
	)
	parameters.MinSuccessfulTests = 100
	parameters.Rng.Seed(seed)

	enc := NewEncoder(time.Time{}, testEncodingOptions)
	iter := NewIterator(nil, nil, testEncodingOptions).(*iterator)
	props.Property("Encoded data should be readable", prop.ForAll(func(input propTestInput) (bool, error) {
		if len(input.messages) == 0 {
			return true, nil
		}
		if debugLogs {
			fmt.Println("---------------------------------------------------")
		}

		var (
			start       = time.Now()
			schemaDescr = namespace.GetTestSchemaDescr(input.schema)

			originalStream  xio.SegmentReader
			originalSegment ts.Segment
			messageTimes    []time.Time
			messageBytes    [][]byte
		)

		// Unfortunately the current implementation is only guaranteed to generate the exact stream
		// for the same input Protobuf messages if the marshaled protobuf bytes are the same. The reason
		// for that is that the Protobuf encoding format is not bijective (for example, fields do not have
		// to be encoded in sorted order by field number and map values can appear in arbitrary orders).
		//
		// The protobuf encoder/iterator compensates for some of these limitations (like the lack of sorting
		// on field order by performing the sort itself during "custom unmarshaling") but does not compensate
		// for others (like the fact that maps are not sorted). Because of these limitations the current encoder
		// may generate different streams (even when provided the same messages) if the messages are not first
		// marshaled into the same exact byte streams.
		for i, m := range input.messages {
			if debugLogs {
				printMessage(fmt.Sprintf("encoding %d", i), m.message)
			}
			clone := dynamic.NewMessage(input.schema)
			clone.MergeFrom(m.message)
			mBytes, err := clone.Marshal()
			if err != nil {
				return false, fmt.Errorf("error marshalling proto message: %v", err)
			}
			// Generate times up-front so they're the same for each iteration.
			messageTimes = append(messageTimes, time.Now())
			messageBytes = append(messageBytes, mBytes)
		}

		ctx := context.NewBackground()
		defer ctx.Close()

		// First verify that if the same input byte slices are passed then the same stream will always
		// be generated.
		for i := 0; i < 10; i++ {
			enc.Reset(start, 0, schemaDescr)
			for j, mBytes := range messageBytes {
				err := enc.Encode(ts.Datapoint{Timestamp: messageTimes[j]}, xtime.Nanosecond, mBytes)
				if err != nil {
					return false, fmt.Errorf(
						"error encoding message: %v, schema: %s", err, input.schema.String())
				}
			}

			currStream, ok := enc.Stream(ctx)
			if !ok {
				return false, fmt.Errorf("encoder returned empty stream")
			}
			currSegment, err := currStream.Segment()
			if err != nil {
				return false, fmt.Errorf("error getting segment: %v", err)
			}

			if originalStream == nil {
				originalStream = currStream
				originalSegment = currSegment
			} else {
				if err := compareSegments(originalSegment, currSegment); err != nil {
					return false, fmt.Errorf("error comparing segments for re-encoding original stream: %v", err)
				}
			}
		}

		// Next verify that re-encoding a stream (after decoding/iterating it) will also always generate the
		// same original stream.
		for i := 0; i < 10; i++ {
			originalStream.Reset(originalSegment)
			iter.Reset(originalStream, schemaDescr)
			enc.Reset(start, 0, schemaDescr)
			j := 0
			for iter.Next() {
				dp, unit, annotation := iter.Current()
				if debugLogs {
					fmt.Println("iterating", dp, unit, annotation)
				}
				if err := enc.Encode(dp, unit, annotation); err != nil {
					return false, fmt.Errorf("error encoding current value")
				}
				j++
			}
			if iter.Err() != nil {
				return false, fmt.Errorf(
					"iteration error: %v, schema: %s", iter.Err(), input.schema.String())
			}

			if j != len(input.messages) {
				return false, fmt.Errorf("expected %d messages but got %d", len(input.messages), j)
			}

			currStream, ok := enc.Stream(ctx)
			if !ok {
				return false, fmt.Errorf("encoder returned empty stream")
			}
			currSegment, err := currStream.Segment()
			if err != nil {
				return false, fmt.Errorf("error getting segment: %v", err)
			}

			if err := compareSegments(originalSegment, currSegment); err != nil {
				return false, fmt.Errorf("error comparing segments for re-encoding original stream after iterating: %v", err)
			}
		}

		return true, nil
	}, genPropTestInputs()))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

type propTestInput struct {
	schema   *desc.MessageDescriptor
	messages []messageAndTimeUnit
}

type messageAndTimeUnit struct {
	message  *dynamic.Message
	timeUnit xtime.Unit
}

// generatedWrite contains numFields values for every type of ProtoBuf
// field. This allows us to generate data independent of the schema itself
// which we may not have seen in the input generation section yet. For example,
// if the maximum number of fields that a message can contain is 10, then this
// struct will generate a slice of size 10 for each of the scalar types and populate
// them with random values.
//
// If we receive a schema where the message has 5 boolean fields and then 5 string
// fields, then we will populate the first 5 booleans fields with generatedWrite.bools[:5]
// and the next 5 string fields with generatedWrite.strings[5:].
type generatedWrite struct {
	timeUnit xtime.Unit

	// Whether we should use one of the randomly generated values in the slice below,
	// or just the default value for the given type.
	useDefaultValue []bool

	bools    []bool
	enums    []int32
	strings  []string
	float32s []float32
	float64s []float64
	int8s    []int8
	int16s   []int16
	int32s   []int32
	int64s   []int64
	uint8s   []uint8
	uint16s  []uint16
	uint32s  []uint32
	uint64s  []uint64
}

func genPropTestInputs() gopter.Gen {
	curriedGenPropTestInput := func(input interface{}) gopter.Gen {
		var (
			inputs      = input.([]interface{})
			numFields   = inputs[0].(int)
			numMessages = inputs[1].(int)
		)

		return genSchema(numFields).FlatMap(
			func(input interface{}) gopter.Gen {
				schema := input.(*desc.MessageDescriptor)
				return genPropTestInput(schema, numMessages)
			}, reflect.TypeOf(propTestInput{}))
	}

	return gopter.CombineGens(
		gen.IntRange(0, maxNumFields),
		gen.IntRange(0, maxNumMessages),
	).FlatMap(curriedGenPropTestInput, reflect.TypeOf(propTestInput{}))
}

func genPropTestInput(schema *desc.MessageDescriptor, numMessages int) gopter.Gen {
	return gopter.CombineGens(
		// Messages to write for the given schema.
		gen.SliceOfN(numMessages, genMessage(schema)),
		// [][]bool that indicates on a field-by-field basis for each message whether the
		// value should be the same as the previous message for that field to ensure we
		// aggressively exercise that codepath.
		gen.SliceOfN(numMessages, gen.SliceOfN(len(schema.GetFields()), gen.Bool())),
	).Map(func(input []interface{}) propTestInput {

		messages := input[0].([]messageAndTimeUnit)
		perMessageShouldBeSameAsPrevWrite := input[1].([][]bool)
		for i, messageAndUnit := range messages {
			m := messageAndUnit.message
			if i == 0 {
				// Can't make the same as previous if there is no previous.
				continue
			}

			perFieldShouldBeSameAsPrevWrite := perMessageShouldBeSameAsPrevWrite[i]
			fields := m.GetKnownFields()
			for j, field := range fields {
				if perFieldShouldBeSameAsPrevWrite[j] {
					fieldNumInt := int(field.GetNumber())
					prevFieldVal := messages[i-1].message.GetFieldByNumber(fieldNumInt)
					m.SetFieldByNumber(fieldNumInt, prevFieldVal)
				}
			}
		}

		return propTestInput{
			schema:   schema,
			messages: messages,
		}
	})
}

func genMessage(schema *desc.MessageDescriptor) gopter.Gen {
	return genWrite().Map(func(input generatedWrite) messageAndTimeUnit {
		return newMessageWithValues(schema, input)
	})
}

func newMessageWithValues(schema *desc.MessageDescriptor, input generatedWrite) messageAndTimeUnit {
	message := dynamic.NewMessage(schema)
	for i, field := range message.GetKnownFields() {
		fieldNumber := int(field.GetNumber())
		switch {
		case input.useDefaultValue[i]:
			// Due to the way ProtoBuf encoding works where there is no way to
			// distinguish between an "unset" field and a field set to its default
			// value, we intentionally force some of the values to their default values
			// to exercise those code paths. This is important because the probability of
			// randomly generating a uint64 with the default value of zero is so unlikely
			// that it will basically never happen.
			continue
		case field.IsMap():
			// Maps require special handling because the type will look like a message, they'll
			// have the repeated label, and to construct them properly we need to look at both
			// the key type as well as the value type.
			var (
				mapKeyType   = field.GetMapKeyType()
				mapValueType = field.GetMapValueType()

				mapKeysForTypeIFace, _   = valuesOfType(nil, i, mapKeyType, input)
				mapValuesForTypeIFace, _ = valuesOfType(nil, i, mapValueType, input)
				mapKeysForType           = interfaceSlice(mapKeysForTypeIFace)
				mapValuesForType         = interfaceSlice(mapValuesForTypeIFace)
			)
			for j, key := range mapKeysForType {
				if messageAndTU, ok := mapValuesForType[j].(messageAndTimeUnit); ok {
					message.PutMapFieldByNumber(fieldNumber, key, messageAndTU.message)
				} else {
					message.PutMapFieldByNumber(fieldNumber, key, mapValuesForType[j])
				}
			}
		default:
			sliceValues, singleValue := valuesOfType(schema, i, field, input)
			if field.IsRepeated() {
				valuesToSet := interfaceSlice(sliceValues)
				if _, ok := singleValue.(messageAndTimeUnit); ok {
					// If its a slice of messageAndTimeUnit (indiciating a field with repeated
					// nested messages) then we need to convert it to a slice of *dynamic.Message.
					valuesToSet = make([]interface{}, 0, len(valuesToSet))
					for _, v := range interfaceSlice(sliceValues) {
						valuesToSet = append(valuesToSet, v.(messageAndTimeUnit).message)
					}
				}

				message.SetFieldByNumber(fieldNumber, valuesToSet)
			} else {
				if messageAndTU, ok := singleValue.(messageAndTimeUnit); ok {
					message.SetFieldByNumber(fieldNumber, messageAndTU.message)
				} else {
					message.SetFieldByNumber(fieldNumber, singleValue)
				}
			}
		}
	}

	// Basic sanity test to protect against bugs in the underlying library:
	// https://github.com/jhump/protoreflect/issues/181
	marshalled, err := message.Marshal()
	if err != nil {
		panic(err)
	}
	unmarshalled := dynamic.NewMessage(schema)
	err = unmarshalled.Unmarshal(marshalled)
	if err != nil {
		panic(err)
	}
	if !dynamic.MessagesEqual(message, unmarshalled) {
		panic("generated message that is not equal after being marshalled and unmarshalled")
	}

	return messageAndTimeUnit{
		message:  message,
		timeUnit: input.timeUnit,
	}
}

func valuesOfType(
	schema *desc.MessageDescriptor,
	i int,
	field *desc.FieldDescriptor,
	input generatedWrite) (interface{}, interface{}) {
	fieldType := field.GetType()

	switch fieldType {
	case dpb.FieldDescriptorProto_TYPE_BOOL:
		return input.bools[0 : i+1], input.bools[i]
	case dpb.FieldDescriptorProto_TYPE_ENUM:
		return input.enums[0 : i+1], input.enums[i]
	case dpb.FieldDescriptorProto_TYPE_BYTES:
		bytesSlice := make([][]byte, 0, i)
		for _, s := range input.strings[0 : i+1] {
			bytesSlice = append(bytesSlice, []byte(s))
		}
		return bytesSlice, []byte(input.strings[i])
	case dpb.FieldDescriptorProto_TYPE_STRING:
		return input.strings[0 : i+1], input.strings[i]
	case dpb.FieldDescriptorProto_TYPE_DOUBLE:
		return input.float64s[0 : i+1], input.float64s[i]
	case dpb.FieldDescriptorProto_TYPE_FLOAT:
		return input.float32s[0 : i+1], input.float32s[i]
	case dpb.FieldDescriptorProto_TYPE_SFIXED32:
		fallthrough
	case dpb.FieldDescriptorProto_TYPE_SINT32:
		fallthrough
	case dpb.FieldDescriptorProto_TYPE_INT32:
		return input.int32s[0 : i+1], input.int32s[i]
	case dpb.FieldDescriptorProto_TYPE_SFIXED64:
		fallthrough
	case dpb.FieldDescriptorProto_TYPE_SINT64:
		fallthrough
	case dpb.FieldDescriptorProto_TYPE_INT64:
		return input.int64s[0 : i+1], input.int64s[i]
	case dpb.FieldDescriptorProto_TYPE_FIXED32:
		fallthrough
	case dpb.FieldDescriptorProto_TYPE_UINT32:
		return input.uint32s[0 : i+1], input.uint32s[i]
	case dpb.FieldDescriptorProto_TYPE_FIXED64:
		fallthrough
	case dpb.FieldDescriptorProto_TYPE_UINT64:
		return input.uint64s[0 : i+1], input.uint64s[i]
	case dpb.FieldDescriptorProto_TYPE_MESSAGE:
		// If the field is a nested message, figure out what the nested schema is
		// and then generate another message to use as the value of that field. We
		// reuse the same inputs value for simplicity.
		//
		// If the schema is set to nil, that means that field itself is a message type
		// that we'd like to generate a value for so get the nested message schema
		// from the field itself instead of looking it up.
		var nestedMessageSchema *desc.MessageDescriptor
		if schema != nil {
			nestedMessageSchema = schema.FindFieldByNumber(field.GetNumber()).GetMessageType()
		} else {
			nestedMessageSchema = field.GetMessageType()
		}
		nestedMessages := make([]messageAndTimeUnit, 0, i)
		for j := 0; j <= i; j++ {
			nestedMessages = append(nestedMessages, newMessageWithValues(nestedMessageSchema, input))
		}
		return nestedMessages, newMessageWithValues(nestedMessageSchema, input)
	default:
		panic(fmt.Sprintf("invalid field type in schema: %v", fieldType))
	}
}

func genWrite() gopter.Gen {
	return gopter.CombineGens(
		genTimeUnit(),
		gen.SliceOfN(maxNumFields, gen.Bool()),
		gen.SliceOfN(maxNumFields, gen.Bool()),
		gen.SliceOfN(maxNumFields, gen.Int32Range(0, int32(maxNumEnumValues)-1)),
		gen.SliceOfN(maxNumFields, gen.Identifier()),
		gen.SliceOfN(maxNumFields, gen.Float32()),
		gen.SliceOfN(maxNumFields, gen.Float64()),
		gen.SliceOfN(maxNumFields, gen.Int8()),
		gen.SliceOfN(maxNumFields, gen.Int16()),
		gen.SliceOfN(maxNumFields, gen.Int32()),
		gen.SliceOfN(maxNumFields, gen.Int64()),
		gen.SliceOfN(maxNumFields, gen.UInt8()),
		gen.SliceOfN(maxNumFields, gen.UInt16()),
		gen.SliceOfN(maxNumFields, gen.UInt32()),
		gen.SliceOfN(maxNumFields, gen.UInt64()),
	).Map(func(input []interface{}) generatedWrite {
		return generatedWrite{
			timeUnit:        input[0].(xtime.Unit),
			useDefaultValue: input[1].([]bool),
			bools:           input[2].([]bool),
			enums:           input[3].([]int32),
			strings:         input[4].([]string),
			float32s:        input[5].([]float32),
			float64s:        input[6].([]float64),
			int8s:           input[7].([]int8),
			int16s:          input[8].([]int16),
			int32s:          input[9].([]int32),
			int64s:          input[10].([]int64),
			uint8s:          input[11].([]uint8),
			uint16s:         input[12].([]uint16),
			uint32s:         input[13].([]uint32),
			uint64s:         input[14].([]uint64),
		}
	})
}

func genSchema(numFields int) gopter.Gen {
	return gopter.CombineGens(
		gen.SliceOfN(numFields, genFieldModifier()),
		gen.SliceOfN(numFields, genMapKeyType()),
		gen.SliceOfN(numFields, genFieldTypeWithNestedMessage()),
		gen.SliceOfN(numFields, genFieldTypeWithNoNestedMessage()),
	).
		Map(func(input []interface{}) *desc.MessageDescriptor {
			var (
				fieldModifiers = input[0].([]fieldModifierProp)
				mapKeyTypes    = input[1].([]dpb.FieldDescriptorProto_Type)
				// fieldTypes are generated with the possibility of a field being a nested message
				// where nestedFieldTypes are generated without the possibility of a field being
				// a nested message to prevent infinite recursion. This limits the property testing
				// of nested message types to a maximum depth of 1, but in practice it doesn't matter
				// much because nested messages are handled by the ProtoBuf specification not our
				// custom encoding.
				fieldTypes       = input[2].([]dpb.FieldDescriptorProto_Type)
				nestedFieldTypes = input[3].([]dpb.FieldDescriptorProto_Type)
			)

			schemaBuilder := schemaBuilderFromFieldTypes(fieldModifiers, mapKeyTypes, fieldTypes, nestedFieldTypes)
			schema, err := schemaBuilder.Build()
			if err != nil {
				panic(err)
			}

			return schema
		})
}

func schemaBuilderFromFieldTypes(
	fieldModifiers []fieldModifierProp,
	mapKeyTypes []dpb.FieldDescriptorProto_Type,
	fieldTypes []dpb.FieldDescriptorProto_Type,
	nestedMessageFieldTypes []dpb.FieldDescriptorProto_Type,
) *builder.MessageBuilder {
	var schemaName string
	if nestedMessageFieldTypes != nil {
		schemaName = "schema"
	} else {
		schemaName = "nested_schema"
	}
	schemaBuilder := builder.NewMessage(schemaName)

	for i, fieldType := range fieldTypes {
		var (
			fieldModifier = fieldModifiers[i]
			fieldNum      = int32(i + 1) // Zero not valid.
			fieldBuilder  *builder.FieldBuilder
		)

		switch {
		case fieldModifier == fieldModifierReserved:
			// Sprinkle in some reserved fields to make sure that we handle those
			// without issue.
			schemaBuilder.AddReservedRange(fieldNum, fieldNum)
			continue
		case fieldModifier == fieldModifierMap:
			// Map key types can only be scalar types.
			mapKeyType := builder.FieldTypeScalar(mapKeyTypes[i])
			mapValueType := newBuilderFieldType(
				// All map values should be "regular" I.E not repeated, reserved, or another map.
				fieldNum, fieldType, make([]fieldModifierProp, len(nestedMessageFieldTypes)), mapKeyTypes, nestedMessageFieldTypes)
			mapFieldName := fmt.Sprintf("_map_%d", fieldNum)
			fieldBuilder = builder.NewMapField(mapFieldName, mapKeyType, mapValueType).SetNumber(fieldNum)
		default:
			builderFieldType := newBuilderFieldType(fieldNum, fieldType, fieldModifiers, mapKeyTypes, nestedMessageFieldTypes)
			fieldBuilder = builder.NewField(fmt.Sprintf("_%d", fieldNum), builderFieldType).
				SetNumber(fieldNum)
		}

		if fieldModifier == fieldModifierRepeated {
			// This is safe because a field cant be repeated and a map by design.
			fieldBuilder = fieldBuilder.SetRepeated()
		}

		schemaBuilder = schemaBuilder.AddField(fieldBuilder)
	}

	return schemaBuilder
}

func newBuilderFieldType(
	fieldNum int32,
	fieldType dpb.FieldDescriptorProto_Type,
	fieldModifiers []fieldModifierProp,
	mapKeyTypes []dpb.FieldDescriptorProto_Type,
	nestedMessageFieldTypes []dpb.FieldDescriptorProto_Type,
) *builder.FieldType {
	if fieldType == dpb.FieldDescriptorProto_TYPE_ENUM {
		var (
			enumFieldName = fmt.Sprintf("_enum_%d", fieldNum)
			enumBuilder   = builder.NewEnum(enumFieldName)
		)
		for j := 0; j < maxNumEnumValues; j++ {
			enumValueName := fmt.Sprintf("_enum_value_%d", j)
			enumBuilder.AddValue(builder.NewEnumValue(enumValueName))
		}
		return builder.FieldTypeEnum(enumBuilder)
	}

	if fieldType == dpb.FieldDescriptorProto_TYPE_MESSAGE {
		// NestedMessageFieldTypes can't contain nested messages so we're limited to a single level
		// of recursion here.
		nestedMessageBuilder := schemaBuilderFromFieldTypes(fieldModifiers, mapKeyTypes, nestedMessageFieldTypes, nil)
		return builder.FieldTypeMessage(nestedMessageBuilder)
	}

	return builder.FieldTypeScalar(fieldType)
}

func genTimeUnit() gopter.Gen {
	return gen.OneConstOf(
		xtime.Second, xtime.Millisecond, xtime.Microsecond, xtime.Nanosecond)
}

func genFieldModifier() gopter.Gen {
	return gen.OneConstOf(
		fieldModifierRegular,
		fieldModifierReserved,
		fieldModifierRepeated,
		fieldModifierMap)
}

func genMapKeyType() gopter.Gen {
	return gen.OneConstOf(validMapKeyTypes...)
}

func genFieldTypeWithNoNestedMessage() gopter.Gen {
	return gen.OneConstOf(allowedProtoTypesSliceIface...)
}

func genFieldTypeWithNestedMessage() gopter.Gen {
	allowedProtoTypesSliceIfaceWithNestedMessage := allowedProtoTypesSliceIface[:] // Shallow copy.
	allowedProtoTypesSliceIfaceWithNestedMessage = append(
		allowedProtoTypesSliceIfaceWithNestedMessage,
		dpb.FieldDescriptorProto_TYPE_MESSAGE)
	return gen.OneConstOf(allowedProtoTypesSliceIfaceWithNestedMessage...)
}

func interfaceSlice(slice interface{}) []interface{} {
	// https://stackoverflow.com/questions/12753805/type-converting-slices-of-interfaces-in-go
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		panic("InterfaceSlice() given a non-slice type")
	}

	ret := make([]interface{}, s.Len())

	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	return ret
}

func printMessage(prefix string, m *dynamic.Message) {
	json, err := m.MarshalJSON()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s: %s\n", prefix, string(json))
}

func compareSegments(a ts.Segment, b ts.Segment) error {
	var (
		aHead []byte
		bHead []byte
		aTail []byte
		bTail []byte
	)
	if a.Head != nil {
		aHead = a.Head.Bytes()
	}
	if b.Head != nil {
		bHead = b.Head.Bytes()
	}
	if a.Tail != nil {
		aTail = a.Tail.Bytes()
	}
	if b.Tail != nil {
		bTail = b.Tail.Bytes()
	}
	if !bytes.Equal(aHead, bHead) {
		return fmt.Errorf(
			"heads do not match. Expected: %v but got: %v",
			aHead, bHead)
	}
	if !bytes.Equal(aTail, bTail) {
		return fmt.Errorf(
			"tails do not match. Expected: %v but got: %v",
			aTail, bTail)
	}
	return nil
}
