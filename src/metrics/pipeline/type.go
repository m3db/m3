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

package pipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/transformation"
	xbytes "github.com/m3db/m3/src/metrics/x/bytes"
)

var (
	errNilAggregationOpProto    = errors.New("nil aggregation op proto message")
	errNilTransformationOpProto = errors.New("nil transformation op proto message")
	errNilRollupOpProto         = errors.New("nil rollup op proto message")
	errNilPipelineProto         = errors.New("nil pipeline proto message")
	errNoOpInUnionMarshaler     = errors.New("no operation in union JSON value")
)

const (
	templateMetricNameVar        = ".MetricName"
	templateOpen                 = "{{"
	templateClose                = "}}"
	templateMetricNameExactMatch = templateOpen + " " + templateMetricNameVar + " " + templateClose
)

var (
	templateMetricNameExactMatchBytes = []byte(templateMetricNameExactMatch)
	templateAllowed                   = []string{templateMetricNameExactMatch}
)

func maybeContainsTemplate(str string) bool {
	return strings.Contains(str, templateOpen) || strings.Contains(str, templateClose)
}

// OpType defines the type of an operation.
type OpType int

// List of supported operation types.
const (
	UnknownOpType OpType = iota
	AggregationOpType
	TransformationOpType
	RollupOpType
)

// AggregationOp is an aggregation operation.
type AggregationOp struct {
	// Type of aggregation performed.
	Type aggregation.Type
}

// NewAggregationOpFromProto creates a new aggregation op from proto.
func NewAggregationOpFromProto(pb *pipelinepb.AggregationOp) (AggregationOp, error) {
	var agg AggregationOp
	if pb == nil {
		return agg, errNilAggregationOpProto
	}
	aggType, err := aggregation.NewTypeFromProto(pb.Type)
	if err != nil {
		return agg, err
	}
	agg.Type = aggType
	return agg, nil
}

// Clone clones the aggregation operation.
func (op AggregationOp) Clone() AggregationOp {
	return op
}

// Equal determines whether two aggregation operations are equal.
func (op AggregationOp) Equal(other AggregationOp) bool {
	return op.Type == other.Type
}

// Proto returns the proto message for the given aggregation operation.
func (op AggregationOp) Proto() (*pipelinepb.AggregationOp, error) {
	pbOpType, err := op.Type.Proto()
	if err != nil {
		return nil, err
	}
	return &pipelinepb.AggregationOp{Type: pbOpType}, nil
}

func (op AggregationOp) String() string {
	return op.Type.String()
}

// MarshalText returns the text encoding of an aggregation operation.
func (op AggregationOp) MarshalText() ([]byte, error) {
	return op.Type.MarshalText()
}

// UnmarshalText unmarshals text-encoded data into an aggregation operation.
func (op *AggregationOp) UnmarshalText(data []byte) error {
	return op.Type.UnmarshalText(data)
}

// TransformationOp is a transformation operation.
type TransformationOp struct {
	// Type of transformation performed.
	Type transformation.Type
}

// NewTransformationOpFromProto creates a new transformation op from proto.
func NewTransformationOpFromProto(pb *pipelinepb.TransformationOp) (TransformationOp, error) {
	var tf TransformationOp
	if err := tf.FromProto(*pb); err != nil {
		return TransformationOp{}, err
	}
	return tf, nil
}

// Equal determines whether two transformation operations are equal.
func (op TransformationOp) Equal(other TransformationOp) bool {
	return op.Type == other.Type
}

// Clone clones the transformation operation.
func (op TransformationOp) Clone() TransformationOp {
	return op
}

// Proto returns the proto message for the given transformation op.
func (op TransformationOp) Proto() (*pipelinepb.TransformationOp, error) {
	var pbOp pipelinepb.TransformationOp
	if err := op.ToProto(&pbOp); err != nil {
		return nil, err
	}
	return &pbOp, nil
}

func (op TransformationOp) String() string {
	return op.Type.String()
}

// ToProto converts the transformation op to a protobuf message in place.
func (op TransformationOp) ToProto(pb *pipelinepb.TransformationOp) error {
	return op.Type.ToProto(&pb.Type)
}

// FromProto converts the protobuf message to a transformation in place.
func (op *TransformationOp) FromProto(pb pipelinepb.TransformationOp) error {
	return op.Type.FromProto(pb.Type)
}

// UnmarshalText extracts this type from its textual representation.
func (op *TransformationOp) UnmarshalText(text []byte) error {
	return op.Type.UnmarshalText(text)
}

// MarshalText serializes this type to its textual representation.
func (op TransformationOp) MarshalText() (text []byte, err error) {
	return op.Type.MarshalText()
}

// RollupType is the rollup type.
// Note: Must match the protobuf enum definition since this is a direct cast.
type RollupType int

const (
	// GroupByRollupType defines the group by rollup op type (default).
	GroupByRollupType RollupType = iota
	// ExcludeByRollupType defines the exclude by rollup op type.
	ExcludeByRollupType
)

// RollupOp is a rollup operation.
type RollupOp struct {
	// Dimensions along which the rollup is performed.
	Tags [][]byte
	// New metric name generated as a result of the rollup.
	newName []byte
	// Type is the rollup type.
	Type RollupType
	// Types of aggregation performed within each unique dimension combination.
	AggregationID    aggregation.ID
	newNameTemplated bool
}

// NewRollupOpFromProto creates a new rollup op from proto.
// NB: the rollup tags are always sorted on construction.
func NewRollupOpFromProto(pb *pipelinepb.RollupOp) (RollupOp, error) {
	var rollup RollupOp
	if pb == nil {
		return rollup, errNilRollupOpProto
	}

	aggregationID, err := aggregation.NewIDFromProto(pb.AggregationTypes)
	if err != nil {
		return rollup, err
	}

	return NewRollupOp(RollupType(pb.Type), pb.NewName, pb.Tags, aggregationID)
}

// NewRollupOp creates a new rollup op.
func NewRollupOp(
	rollupType RollupType,
	rollupNewName string,
	rollupTags []string,
	rollupAggregationID aggregation.ID,
) (RollupOp, error) {
	var rollup RollupOp

	tags := make([]string, len(rollupTags))
	copy(tags, rollupTags)
	sort.Strings(tags)

	var newNameTemplated bool
	if maybeContainsTemplate(rollupNewName) {
		// This metric might have a templated metric name.
		newNameTemplated = true

		// Right now only support "{{ .MetricName }}" to be able to generate
		// the resulting metric name without using a Go template and only
		// a single instance of it.
		if n := strings.Count(rollupNewName, templateMetricNameExactMatch); n > 1 {
			return rollup, fmt.Errorf(
				"rollup contained template variable metric name more than once: "+
					"input=%s, count_var_metric_name=%v", rollupNewName, n)
		}

		// Replace and see if all template tags resolved.
		replacedNewName := strings.Replace(rollupNewName, templateMetricNameExactMatch, "", 1)

		// Make sure fully replaced all instances of template usage, otherwise
		// there are some other variables not supported or invalid use of
		// template variable tags.
		if maybeContainsTemplate(replacedNewName) {
			return rollup, fmt.Errorf(
				"rollup contained template tags but variables not resolved: "+
					"input=%s, allowed=%v", rollupNewName, templateAllowed)
		}
	}

	return RollupOp{
		Type:             rollupType,
		Tags:             xbytes.ArraysFromStringArray(tags),
		AggregationID:    rollupAggregationID,
		newName:          []byte(rollupNewName),
		newNameTemplated: newNameTemplated,
	}, nil
}

// NewName returns the new rollup name based on an existing name if
// the new name uses a template, or otherwise the literal new name.
func (op RollupOp) NewName(currName []byte) []byte {
	if !op.newNameTemplated {
		// No templated name, just return the "literal" new name.
		return op.newName
	}

	out := make([]byte, 0, len(op.newName)+len(currName))
	idx := bytes.Index(op.newName, templateMetricNameExactMatchBytes)
	if idx == -1 {
		return op.newName
	}

	out = append(out, op.newName[0:idx]...)
	out = append(out, currName...)
	out = append(out, op.newName[idx+len(templateMetricNameExactMatchBytes):]...)
	return out
}

// SameTransform returns true if the two rollup operations have the same rollup transformation
// (i.e., same new rollup metric name and same set of rollup tags).
func (op RollupOp) SameTransform(other RollupOp) bool {
	if len(op.Tags) != len(other.Tags) {
		return false
	}
	if !bytes.Equal(op.newName, other.newName) {
		return false
	}
	// Sort the tags and compare.
	clonedTags := xbytes.ArraysToStringArray(op.Tags)
	sort.Strings(clonedTags)
	otherClonedTags := xbytes.ArraysToStringArray(other.Tags)
	sort.Strings(otherClonedTags)
	for i := 0; i < len(clonedTags); i++ {
		if clonedTags[i] != otherClonedTags[i] {
			return false
		}
	}
	return true
}

// Equal returns true if two rollup operations are equal.
func (op RollupOp) Equal(other RollupOp) bool {
	if !op.AggregationID.Equal(other.AggregationID) {
		return false
	}
	if op.Type != other.Type {
		return false
	}
	return op.SameTransform(other)
}

// Clone clones the rollup operation.
func (op RollupOp) Clone() RollupOp {
	newName := make([]byte, len(op.newName))
	copy(newName, op.newName)
	return RollupOp{
		Type:             op.Type,
		Tags:             xbytes.ArrayCopy(op.Tags),
		AggregationID:    op.AggregationID,
		newName:          newName,
		newNameTemplated: op.newNameTemplated,
	}
}

// Proto returns the proto message for the given rollup op.
func (op RollupOp) Proto() (*pipelinepb.RollupOp, error) {
	aggTypes, err := op.AggregationID.Types()
	if err != nil {
		return nil, err
	}
	pbAggTypes, err := aggTypes.Proto()
	if err != nil {
		return nil, err
	}
	return &pipelinepb.RollupOp{
		Type:             pipelinepb.RollupOp_Type(op.Type),
		NewName:          string(op.newName),
		Tags:             xbytes.ArraysToStringArray(op.Tags),
		AggregationTypes: pbAggTypes,
	}, nil
}

func (op RollupOp) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	fmt.Fprintf(&b, "name: %s, ", op.newName)
	fmt.Fprintf(&b, "type: %v, ", op.Type)
	b.WriteString("tags: [")
	for i, t := range op.Tags {
		fmt.Fprintf(&b, "%s", t)
		if i < len(op.Tags)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("], ")
	fmt.Fprintf(&b, "aggregation: %v", op.AggregationID)
	b.WriteString("}")
	return b.String()
}

// MarshalJSON returns the JSON encoding of a rollup operation.
func (op RollupOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(newRollupMarshaler(op))
}

// UnmarshalJSON unmarshals JSON-encoded data into a rollup operation.
func (op *RollupOp) UnmarshalJSON(data []byte) error {
	var converted rollupMarshaler
	err := json.Unmarshal(data, &converted)
	if err != nil {
		return err
	}
	*op, err = converted.RollupOp()
	return err
}

// UnmarshalYAML unmarshals YAML-encoded data into a rollup operation.
func (op *RollupOp) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var converted rollupMarshaler
	err := unmarshal(&converted)
	if err != nil {
		return err
	}
	*op, err = converted.RollupOp()
	return err
}

// MarshalYAML returns the YAML representation of this type.
func (op RollupOp) MarshalYAML() (interface{}, error) {
	return newRollupMarshaler(op), nil
}

type rollupMarshaler struct {
	NewName       string         `json:"newName" yaml:"newName"`
	Tags          []string       `json:"tags" yaml:"tags"`
	Type          RollupType     `json:"type" yaml:"type"`
	AggregationID aggregation.ID `json:"aggregation,omitempty" yaml:"aggregation"`
}

func newRollupMarshaler(op RollupOp) rollupMarshaler {
	return rollupMarshaler{
		Type:          op.Type,
		NewName:       string(op.newName),
		Tags:          xbytes.ArraysToStringArray(op.Tags),
		AggregationID: op.AggregationID,
	}
}

func (m rollupMarshaler) RollupOp() (RollupOp, error) {
	return NewRollupOp(m.Type, m.NewName, m.Tags, m.AggregationID)
}

// OpUnion is a union of different types of operation.
type OpUnion struct {
	Type           OpType
	Aggregation    AggregationOp
	Transformation TransformationOp
	Rollup         RollupOp
}

// NewOpUnionFromProto creates a new operation union from proto.
func NewOpUnionFromProto(pb pipelinepb.PipelineOp) (OpUnion, error) {
	var (
		u   OpUnion
		err error
	)
	switch pb.Type {
	case pipelinepb.PipelineOp_AGGREGATION:
		u.Type = AggregationOpType
		u.Aggregation, err = NewAggregationOpFromProto(pb.Aggregation)
	case pipelinepb.PipelineOp_TRANSFORMATION:
		u.Type = TransformationOpType
		u.Transformation, err = NewTransformationOpFromProto(pb.Transformation)
	case pipelinepb.PipelineOp_ROLLUP:
		u.Type = RollupOpType
		u.Rollup, err = NewRollupOpFromProto(pb.Rollup)
	default:
		err = fmt.Errorf("unknown op type in proto: %v", pb.Type)
	}
	return u, err
}

// Equal determines whether two operation unions are equal.
func (u OpUnion) Equal(other OpUnion) bool {
	if u.Type != other.Type {
		return false
	}
	switch u.Type {
	case AggregationOpType:
		return u.Aggregation.Equal(other.Aggregation)
	case TransformationOpType:
		return u.Transformation.Equal(other.Transformation)
	case RollupOpType:
		return u.Rollup.Equal(other.Rollup)
	}
	return true
}

// Clone clones an operation union.
func (u OpUnion) Clone() OpUnion {
	clone := OpUnion{Type: u.Type}
	switch u.Type {
	case AggregationOpType:
		clone.Aggregation = u.Aggregation.Clone()
	case TransformationOpType:
		clone.Transformation = u.Transformation.Clone()
	case RollupOpType:
		clone.Rollup = u.Rollup.Clone()
	}
	return clone
}

// Proto creates a proto message for the given operation.
func (u OpUnion) Proto() (*pipelinepb.PipelineOp, error) {
	var (
		pbOp pipelinepb.PipelineOp
		err  error
	)
	switch u.Type {
	case AggregationOpType:
		pbOp.Type = pipelinepb.PipelineOp_AGGREGATION
		pbOp.Aggregation, err = u.Aggregation.Proto()
	case TransformationOpType:
		pbOp.Type = pipelinepb.PipelineOp_TRANSFORMATION
		pbOp.Transformation, err = u.Transformation.Proto()
	case RollupOpType:
		pbOp.Type = pipelinepb.PipelineOp_ROLLUP
		pbOp.Rollup, err = u.Rollup.Proto()
	default:
		err = fmt.Errorf("unknown op type: %v", u.Type)
	}
	return &pbOp, err
}

func (u OpUnion) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	switch u.Type {
	case AggregationOpType:
		fmt.Fprintf(&b, "aggregation: %s", u.Aggregation.String())
	case TransformationOpType:
		fmt.Fprintf(&b, "transformation: %s", u.Transformation.String())
	case RollupOpType:
		fmt.Fprintf(&b, "rollup: %s", u.Rollup.String())
	default:
		fmt.Fprintf(&b, "unknown op type: %v", u.Type)
	}
	b.WriteString("}")
	return b.String()
}

// MarshalJSON returns the JSON encoding of an operation union.
func (u OpUnion) MarshalJSON() ([]byte, error) {
	converted, err := newUnionMarshaler(u)
	if err != nil {
		return nil, err
	}
	return json.Marshal(converted)
}

// UnmarshalJSON unmarshals JSON-encoded data into an operation union.
func (u *OpUnion) UnmarshalJSON(data []byte) error {
	var converted unionMarshaler
	if err := json.Unmarshal(data, &converted); err != nil {
		return err
	}
	union, err := converted.OpUnion()
	if err != nil {
		return err
	}
	*u = union
	return nil
}

// MarshalJSON returns the JSON encoding of an operation union.
func (u OpUnion) MarshalYAML() (interface{}, error) {
	return newUnionMarshaler(u)
}

// UnmarshalYAML unmarshals YAML-encoded data into an operation union.
func (u *OpUnion) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var converted unionMarshaler
	if err := unmarshal(&converted); err != nil {
		return err
	}
	union, err := converted.OpUnion()
	if err != nil {
		return err
	}
	*u = union
	return nil
}

// unionMarshaler is a helper type to facilitate marshaling and unmarshaling operation unions.
type unionMarshaler struct {
	Aggregation    *AggregationOp    `json:"aggregation,omitempty" yaml:"aggregation"`
	Transformation *TransformationOp `json:"transformation,omitempty" yaml:"transformation"`
	Rollup         *RollupOp         `json:"rollup,omitempty" yaml:"rollup"`
}

func newUnionMarshaler(u OpUnion) (unionMarshaler, error) {
	var converted unionMarshaler
	switch u.Type {
	case AggregationOpType:
		converted.Aggregation = &u.Aggregation
	case TransformationOpType:
		converted.Transformation = &u.Transformation
	case RollupOpType:
		converted.Rollup = &u.Rollup
	default:
		return unionMarshaler{}, fmt.Errorf("unknown op type: %v", u.Type)
	}
	return converted, nil
}

func (m unionMarshaler) OpUnion() (OpUnion, error) {
	if m.Aggregation != nil {
		return OpUnion{Type: AggregationOpType, Aggregation: *m.Aggregation}, nil
	}
	if m.Transformation != nil {
		return OpUnion{Type: TransformationOpType, Transformation: *m.Transformation}, nil
	}
	if m.Rollup != nil {
		return OpUnion{Type: RollupOpType, Rollup: *m.Rollup}, nil
	}
	return OpUnion{}, errNoOpInUnionMarshaler
}

// Pipeline is a pipeline of operations.
type Pipeline struct {
	// a list of pipeline operations.
	operations []OpUnion
}

// NewPipeline creates a new pipeline.
func NewPipeline(ops []OpUnion) Pipeline {
	return Pipeline{operations: ops}
}

// NewPipelineFromProto creates a new pipeline from proto.
func NewPipelineFromProto(pb *pipelinepb.Pipeline) (Pipeline, error) {
	if pb == nil {
		return Pipeline{}, errNilPipelineProto
	}
	operations := make([]OpUnion, 0, len(pb.Ops))
	for _, pbOp := range pb.Ops {
		operation, err := NewOpUnionFromProto(pbOp)
		if err != nil {
			return Pipeline{}, err
		}
		operations = append(operations, operation)
	}
	return Pipeline{operations: operations}, nil
}

// Len returns the number of steps in a pipeline.
func (p Pipeline) Len() int { return len(p.operations) }

// IsEmpty determines whether a pipeline is empty.
func (p Pipeline) IsEmpty() bool { return p.Len() == 0 }

// At returns the operation at a given step.
func (p Pipeline) At(i int) OpUnion { return p.operations[i] }

// Equal determines whether two pipelines are equal.
func (p Pipeline) Equal(other Pipeline) bool {
	if len(p.operations) != len(other.operations) {
		return false
	}
	for i := 0; i < len(p.operations); i++ {
		if !p.operations[i].Equal(other.operations[i]) {
			return false
		}
	}
	return true
}

// SubPipeline returns a sub-pipeline containing operations between step `startInclusive`
// and step `endExclusive` of the current pipeline.
func (p Pipeline) SubPipeline(startInclusive int, endExclusive int) Pipeline {
	return Pipeline{operations: p.operations[startInclusive:endExclusive]}
}

// Clone clones the pipeline.
func (p Pipeline) Clone() Pipeline {
	clone := make([]OpUnion, len(p.operations))
	for i, op := range p.operations {
		clone[i] = op.Clone()
	}
	return Pipeline{operations: clone}
}

// Proto returns the proto message for a given pipeline.
func (p Pipeline) Proto() (*pipelinepb.Pipeline, error) {
	pbOps := make([]pipelinepb.PipelineOp, 0, len(p.operations))
	for _, op := range p.operations {
		pbOp, err := op.Proto()
		if err != nil {
			return nil, err
		}
		pbOps = append(pbOps, *pbOp)
	}
	return &pipelinepb.Pipeline{Ops: pbOps}, nil
}

func (p Pipeline) String() string {
	var b bytes.Buffer
	b.WriteString("{operations: [")
	for i, op := range p.operations {
		b.WriteString(op.String())
		if i < len(p.operations)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]}")
	return b.String()
}

// MarshalJSON returns the JSON encoding of a pipeline.
func (p Pipeline) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.operations)
}

// UnmarshalJSON unmarshals JSON-encoded data into a pipeline.
func (p *Pipeline) UnmarshalJSON(data []byte) error {
	var operations []OpUnion
	if err := json.Unmarshal(data, &operations); err != nil {
		return err
	}
	p.operations = operations
	return nil
}

// UnmarshalYAML unmarshals YAML-encoded data into a pipeline.
func (p *Pipeline) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var operations []OpUnion
	if err := unmarshal(&operations); err != nil {
		return err
	}
	p.operations = operations
	return nil
}

// MarshalYAML returns the YAML representation.
func (p Pipeline) MarshalYAML() (interface{}, error) {
	return p.operations, nil
}
