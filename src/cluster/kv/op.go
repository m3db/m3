package kv

import "github.com/golang/protobuf/proto"

type opBase struct {
	ot  OpType
	key string
}

func newOpBase(t OpType, key string) opBase {
	return opBase{ot: t, key: key}
}

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
	return SetOp{opBase: newOpBase(Set, key), Value: value}
}

type opResponse struct {
	opBase

	value interface{}
}

func newOpResponse(key string, value interface{}) OpResponse {
	return opResponse{opBase: newOpBase(Set, key), value: value}
}

func (r opResponse) Value() interface{} {
	return r.value
}
