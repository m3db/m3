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
	"testing"
	"time"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/builder"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

var maxNumFields = 10
var maxNumMessages = 1000

func TestRoundtripProp(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
	)
	parameters.MinSuccessfulTests = 40
	parameters.Rng.Seed(seed)

	props.Property("Encoded data should be readable", prop.ForAll(func(input propTestInput) (bool, error) {
		enc, err := NewEncoder(nil, input.schema, testEncodingOptions)
		if err != nil {
			return false, fmt.Errorf("error constructing encoder: %v", err)
		}

		for _, m := range input.messages {
			// The encoder will mutate the message so make sure we clone it first.
			clone := dynamic.NewMessage(input.schema)
			clone.MergeFrom(m)

			err := enc.Encode(clone)
			if err != nil {
				return false, fmt.Errorf("error encoding message: %v", err)
			}
		}

		// TODO: Fix this, need a discard method or w/e.
		checkedBytes, _ := enc.stream.Rawbytes()
		rawBytes := checkedBytes.Bytes()
		buff := bytes.NewBuffer(rawBytes)
		iter, err := NewIterator(buff, input.schema)
		if err != nil {
			return false, fmt.Errorf("error constructing iterator: %v", err)
		}

		for i, m := range input.messages {
			iter.Next()
			decodedM := iter.Current()
			if iter.err != nil {
				// TODO: Expose iteration errors?
				return false, fmt.Errorf("iteration error: %v", iter.err)
			}
			for _, field := range m.GetKnownFields() {
				var (
					fieldNum    = int(field.GetNumber())
					expectedVal = m.GetFieldByNumber(fieldNum)
					actualVal   = decodedM.GetFieldByNumber(fieldNum)
				)

				if !fieldsEqual(expectedVal, actualVal) {
					return false, fmt.Errorf(
						"expected %v but got %v on iteration number %d and fieldNum %d",
						expectedVal, actualVal, i, fieldNum)
				}
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
// and the next 5 string fields with generatedWrite.strings[:5].
type generatedWrite struct {
	// Whether we should use one of the randomly generated values in the slice below,
	// or just the default value for the given type.S
	useDefaultValue []bool

	bools    []bool
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
		gen.SliceOfN(numMessages, genMessage(schema)),
	).Map(func(input []interface{}) propTestInput {
		return propTestInput{
			schema:   schema,
			messages: input[0].([]*dynamic.Message),
		}
	})
}

func genMessage(schema *desc.MessageDescriptor) gopter.Gen {
	return genWrite().Map(func(input generatedWrite) *dynamic.Message {
		message := dynamic.NewMessage(schema)
		for i, field := range message.GetKnownFields() {
			if input.useDefaultValue[i] {
				// Due to the way ProtoBuf encoding works where there is no way to
				// distinguish between an "unset" field and a field set to its default
				// value, we intentionally force some of the values to their default values
				// to exercise those code paths. This is important because the probabily of
				// randomly generating a uint64 with the default value of zero is so unlikely
				// that it will basically never happen.
				continue
			}

			var (
				fieldType   = field.GetType()
				fieldNumber = int(field.GetNumber())
			)
			switch fieldType {
			case dpb.FieldDescriptorProto_TYPE_BOOL:
				message.SetFieldByNumber(fieldNumber, input.bools[i])
			case dpb.FieldDescriptorProto_TYPE_BYTES:
				message.SetFieldByNumber(fieldNumber, []byte(input.strings[i]))
			case dpb.FieldDescriptorProto_TYPE_STRING:
				message.SetFieldByNumber(fieldNumber, input.strings[i])
			case dpb.FieldDescriptorProto_TYPE_DOUBLE:
				message.SetFieldByNumber(fieldNumber, input.float64s[i])
			case dpb.FieldDescriptorProto_TYPE_FLOAT:
				message.SetFieldByNumber(fieldNumber, input.float32s[i])
			case dpb.FieldDescriptorProto_TYPE_INT32:
				message.SetFieldByNumber(fieldNumber, input.int32s[i])
			case dpb.FieldDescriptorProto_TYPE_INT64:
				message.SetFieldByNumber(fieldNumber, input.int64s[i])
			case dpb.FieldDescriptorProto_TYPE_UINT32:
				message.SetFieldByNumber(fieldNumber, input.uint32s[i])
			case dpb.FieldDescriptorProto_TYPE_UINT64:
				message.SetFieldByNumber(fieldNumber, input.uint64s[i])
			case dpb.FieldDescriptorProto_TYPE_FIXED64:
				message.SetFieldByNumber(fieldNumber, input.uint64s[i])
			case dpb.FieldDescriptorProto_TYPE_FIXED32:
				message.SetFieldByNumber(fieldNumber, input.uint32s[i])
			case dpb.FieldDescriptorProto_TYPE_SFIXED64:
				message.SetFieldByNumber(fieldNumber, input.int64s[i])
			case dpb.FieldDescriptorProto_TYPE_SFIXED32:
				message.SetFieldByNumber(fieldNumber, input.int32s[i])
			case dpb.FieldDescriptorProto_TYPE_SINT64:
				message.SetFieldByNumber(fieldNumber, input.int64s[i])
			case dpb.FieldDescriptorProto_TYPE_SINT32:
				message.SetFieldByNumber(fieldNumber, input.int32s[i])
			default:
				panic(fmt.Sprintf("invalid field type in schema: %v", fieldType))
			}
		}

		return message
	})
}

func genWrite() gopter.Gen {
	return gopter.CombineGens(
		gen.SliceOfN(maxNumFields, gen.Bool()),
		gen.SliceOfN(maxNumFields, gen.Bool()),
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
			strings:         input[2].([]string),
			float32s:        input[3].([]float32),
			float64s:        input[4].([]float64),
			int8s:           input[5].([]int8),
			int16s:          input[6].([]int16),
			int32s:          input[7].([]int32),
			int64s:          input[8].([]int64),
			uint8s:          input[9].([]uint8),
			uint16s:         input[10].([]uint16),
			uint32s:         input[11].([]uint32),
			uint64s:         input[12].([]uint64),
		}
	})
}

func genSchema(numFields int) gopter.Gen {
	return gen.
		SliceOfN(numFields, genFieldType()).
		Map(func(fieldTypes []dpb.FieldDescriptorProto_Type) *desc.MessageDescriptor {
			schemaBuilder := builder.NewMessage("schema")
			for i, fieldType := range fieldTypes {
				fieldNum := i + 1 // Zero not valid.
				field := builder.NewField(fmt.Sprintf("_%d", fieldNum), builder.FieldTypeScalar(fieldType)).
					SetNumber(int32(fieldNum))
				schemaBuilder = schemaBuilder.AddField(field)
			}
			schema, err := schemaBuilder.Build()
			if err != nil {
				panic(fmt.Errorf("error building dynamic schema message: %v", err))
			}

			return schema
		})
}

func genFieldType() gopter.Gen {
	return gen.OneConstOf(
		// TODO: Use allowed values for this
		dpb.FieldDescriptorProto_TYPE_DOUBLE,
		dpb.FieldDescriptorProto_TYPE_FLOAT,
		dpb.FieldDescriptorProto_TYPE_INT64,
		dpb.FieldDescriptorProto_TYPE_UINT64,
		dpb.FieldDescriptorProto_TYPE_INT32,
		dpb.FieldDescriptorProto_TYPE_FIXED64,
		dpb.FieldDescriptorProto_TYPE_FIXED32,
		dpb.FieldDescriptorProto_TYPE_BOOL,
		dpb.FieldDescriptorProto_TYPE_STRING,
		// FieldDescriptorProto_TYPE_MESSAGE,
		dpb.FieldDescriptorProto_TYPE_BYTES,
		dpb.FieldDescriptorProto_TYPE_UINT32,
		// FieldDescriptorProto_TYPE_ENUM,
		dpb.FieldDescriptorProto_TYPE_SFIXED32,
		dpb.FieldDescriptorProto_TYPE_SFIXED64,
		dpb.FieldDescriptorProto_TYPE_SINT32,
		dpb.FieldDescriptorProto_TYPE_SINT64,
	)
}
