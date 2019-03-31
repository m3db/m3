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
	"fmt"
	"os"
	"reflect"
	"runtime/debug"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3x/time"

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

// TODO(rartoul): Modify this prop test to generate schemas with repeated fields and maps
// (which are basically the same thing) as well as nested messages once we add support for
// those features: https://github.com/m3db/m3/issues/1471
func TestRoundtripProp(t *testing.T) {
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
		for range input.messages {
			currTime = currTime.Add(time.Nanosecond)
			times = append(times, currTime)
		}

		enc.Reset(currTime, 0)
		enc.SetSchema(input.schema)

		for i, m := range input.messages {
			// The encoder will mutate the message so make sure we clone it first.
			clone := dynamic.NewMessage(input.schema)
			clone.MergeFrom(m)
			cloneBytes, err := clone.Marshal()
			if err != nil {
				return false, fmt.Errorf("error marshaling proto message: %v", err)
			}

			if debugLogs {
				printMessage("encoding", m)
			}
			err = enc.Encode(ts.Datapoint{Timestamp: times[i]}, xtime.Nanosecond, cloneBytes)
			if err != nil {
				return false, fmt.Errorf(
					"error encoding message: %v, schema: %s", err, input.schema.String())
			}
		}

		stream := enc.Stream()
		if stream == nil {
			return true, nil
		}

		iter.SetSchema(input.schema)
		iter.Reset(stream)

		i := 0
		for iter.Next() {
			var (
				m                    = input.messages[i]
				dp, unit, annotation = iter.Current()
			)
			decodedM := dynamic.NewMessage(input.schema)
			require.NoError(t, decodedM.Unmarshal(annotation))
			if debugLogs {
				printMessage("decoding", decodedM)
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

		return true, nil
	}, genPropTestInputs()))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

type propTestInput struct {
	schema   *desc.MessageDescriptor
	messages []*dynamic.Message
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
	// Whether we should use one of the randomly generated values in the slice below,
	// or just the default value for the given type.
	useDefaultValue []bool
	// Whether we should use the same value as the previous write.
	usePrevValue []bool

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

		messages := input[0].([]*dynamic.Message)
		perMessageShouldBeSameAsPrevWrite := input[1].([][]bool)
		for i, m := range messages {
			if i == 0 {
				// Can't make the same as previous if there is no previous.
				continue
			}

			perFieldShouldBeSameAsPrevWrite := perMessageShouldBeSameAsPrevWrite[i]
			fields := m.GetKnownFields()
			for j, field := range fields {
				if perFieldShouldBeSameAsPrevWrite[j] {
					fieldNumInt := int(field.GetNumber())
					prevFieldVal := messages[i-1].GetFieldByNumber(fieldNumInt)
					m.SetFieldByNumber(fieldNumInt, prevFieldVal)
				}
			}
		}

		return propTestInput{
			schema:   schema,
			messages: input[0].([]*dynamic.Message),
		}
	})
}

func genMessage(schema *desc.MessageDescriptor) gopter.Gen {
	return genWrite().Map(func(input generatedWrite) *dynamic.Message {
		return newMessageWithValues(schema, input)
	})
}

func newMessageWithValues(schema *desc.MessageDescriptor, input generatedWrite) *dynamic.Message {
	message := dynamic.NewMessage(schema)
	for i, field := range message.GetKnownFields() {
		if input.useDefaultValue[i] {
			// Due to the way ProtoBuf encoding works where there is no way to
			// distinguish between an "unset" field and a field set to its default
			// value, we intentionally force some of the values to their default values
			// to exercise those code paths. This is important because the probability of
			// randomly generating a uint64 with the default value of zero is so unlikely
			// that it will basically never happen.
			continue
		}

		fieldNumber := int(field.GetNumber())
		switch {
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
				message.PutMapFieldByNumber(fieldNumber, key, mapValuesForType[j])
			}
		default:
			sliceValues, singleValue := valuesOfType(schema, i, field, input)
			if field.IsRepeated() {
				message.SetFieldByNumber(fieldNumber, sliceValues)
			} else {
				message.SetFieldByNumber(fieldNumber, singleValue)
			}
		}
	}

	// Basic sanity test to protect against bugs in the underlying library:
	// https://github.com/jhump/protoreflect/issues/181
	marshaled, err := message.Marshal()
	if err != nil {
		panic(err)
	}
	unmarshaled := dynamic.NewMessage(schema)
	err = unmarshaled.Unmarshal(marshaled)
	if err != nil {
		panic(err)
	}
	if !dynamic.MessagesEqual(message, unmarshaled) {
		panic("generated message that is not equal after being marshaled and unmarshaled")
	}

	return message
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
		nestedMessages := make([]*dynamic.Message, 0, i)
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
			useDefaultValue: input[0].([]bool),
			bools:           input[1].([]bool),
			enums:           input[2].([]int32),
			strings:         input[3].([]string),
			float32s:        input[4].([]float32),
			float64s:        input[5].([]float64),
			int8s:           input[6].([]int8),
			int16s:          input[7].([]int16),
			int32s:          input[8].([]int32),
			int64s:          input[9].([]int64),
			uint8s:          input[10].([]uint8),
			uint16s:         input[11].([]uint16),
			uint32s:         input[12].([]uint32),
			uint64s:         input[13].([]uint64),
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

func genFieldModifier() gopter.Gen {
	return gen.OneConstOf(
		fieldModifierRegular, fieldModifierReserved, fieldModifierRepeated, fieldModifierMap)
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
