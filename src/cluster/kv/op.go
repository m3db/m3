package kv

import "github.com/golang/protobuf/proto"

type condition struct {
	targetType  TargetType
	compareType CompareType
	key         string
	value       interface{}
}

// NewCondition returns a new Condition
func NewCondition() Condition { return condition{} }

func (c condition) TargetType() TargetType                 { return c.targetType }
func (c condition) CompareType() CompareType               { return c.compareType }
func (c condition) Key() string                            { return c.key }
func (c condition) Value() interface{}                     { return c.value }
func (c condition) SetTargetType(t TargetType) Condition   { c.targetType = t; return c }
func (c condition) SetCompareType(t CompareType) Condition { c.compareType = t; return c }
func (c condition) SetKey(key string) Condition            { c.key = key; return c }
func (c condition) SetValue(value interface{}) Condition   { c.value = value; return c }

type opBase struct {
	ot  OpType
	key string
}

func newOpBase(t OpType, key string) opBase { return opBase{ot: t, key: key} }

func (r opBase) Type() OpType         { return r.ot }
func (r opBase) Key() string          { return r.key }
func (r opBase) SetType(t OpType) Op  { r.ot = t; return r }
func (r opBase) SetKey(key string) Op { r.key = key; return r }

// SetOp is a Op with OpType Set
type SetOp struct {
	opBase

	Value proto.Message
}

// NewSetOp returns a SetOp
func NewSetOp(key string, value proto.Message) SetOp {
	return SetOp{opBase: newOpBase(OpSet, key), Value: value}
}

type opResponse struct {
	Op

	value interface{}
}

// NewOpResponse creates a new OpResponse
func NewOpResponse(op Op) OpResponse {
	return opResponse{Op: op}
}

func (r opResponse) Value() interface{}                { return r.value }
func (r opResponse) SetValue(v interface{}) OpResponse { r.value = v; return r }

type response struct {
	opr []OpResponse
}

// NewResponse creates a new transaction Response
func NewResponse() Response { return response{} }

func (r response) Responses() []OpResponse                 { return r.opr }
func (r response) SetResponses(oprs []OpResponse) Response { r.opr = oprs; return r }
