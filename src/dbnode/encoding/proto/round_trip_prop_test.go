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
var maxNumMessages = 100

func TestRoundtripProp(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		// seed     = int64(1551649544821181000)
		props    = gopter.NewProperties(parameters)
		reporter = gopter.NewFormatedReporter(true, 160, os.Stdout)
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
		iter, err := NewIterator(buff, input.schema, testEncodingOptions)
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

type generatedWrite struct {
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
			inputs = input.([]interface{})
			schema = inputs[0].(*desc.MessageDescriptor)
		)
		return genPropTestInput(schema)
	}
	return gopter.CombineGens(
		genSchema(),
	).FlatMap(curriedGenPropTestInput, reflect.TypeOf(propTestInput{}))
}

func genPropTestInput(schema *desc.MessageDescriptor) gopter.Gen {
	return gopter.CombineGens(
		// Make this variable
		gen.SliceOfN(maxNumMessages, genMessage(schema)),
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
			fieldType := field.GetType()
			fieldNumber := int(field.GetNumber())

			switch fieldType {
			case dpb.FieldDescriptorProto_TYPE_BOOL:
				message.SetFieldByNumber(fieldNumber, input.bools[i])
			case dpb.FieldDescriptorProto_TYPE_DOUBLE:
				message.SetFieldByNumber(fieldNumber, input.float64s[i])
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
			bools:    input[0].([]bool),
			strings:  input[1].([]string),
			float32s: input[2].([]float32),
			float64s: input[3].([]float64),
			int8s:    input[4].([]int8),
			int16s:   input[5].([]int16),
			int32s:   input[6].([]int32),
			int64s:   input[7].([]int64),
			uint8s:   input[8].([]uint8),
			uint16s:  input[9].([]uint16),
			uint32s:  input[10].([]uint32),
			uint64s:  input[11].([]uint64),
		}
	})
}

func genSchema() gopter.Gen {
	// TODO: Make number of fields variable
	return gen.
		SliceOfN(maxNumFields, genFieldType()).
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
		// dpb.FieldDescriptorProto_TYPE_FLOAT,
		// dpb.FieldDescriptorProto_TYPE_INT64,
		// dpb.FieldDescriptorProto_TYPE_UINT64,
		// dpb.FieldDescriptorProto_TYPE_INT32,
		// dpb.FieldDescriptorProto_TYPE_FIXED64,
		// dpb.FieldDescriptorProto_TYPE_FIXED32,
		dpb.FieldDescriptorProto_TYPE_BOOL,
		// dpb.FieldDescriptorProto_TYPE_STRING,
		// FieldDescriptorProto_TYPE_MESSAGE,
		// dpb.FieldDescriptorProto_TYPE_BYTES,
		// dpb.FieldDescriptorProto_TYPE_UINT32,
		// FieldDescriptorProto_TYPE_ENUM,
		// dpb.FieldDescriptorProto_TYPE_SFIXED32,
		// dpb.FieldDescriptorProto_TYPE_SFIXED64,
		// dpb.FieldDescriptorProto_TYPE_SINT32,
		// dpb.FieldDescriptorProto_TYPE_SINT64,
	)
}
