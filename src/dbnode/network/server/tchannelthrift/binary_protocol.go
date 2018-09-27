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
package tchannelthrift

// Adapted from: https://github.com/apache/thrift/blob/master/lib/go/thrift/binary_protocol.go

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	tthrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/m3db/m3x/pool"
)

type TBinaryProtocol struct {
	trans         tthrift.TRichTransport
	origTransport tthrift.TTransport
	reader        io.Reader
	writer        io.Writer
	strictRead    bool
	strictWrite   bool
	buffer        [64]byte

	bytesPool pool.BytesPool
}

func NewTBinaryProtocolTransport(t tthrift.TTransport, p pool.BytesPool) *TBinaryProtocol {
	return NewTBinaryProtocol(t, p, false, true)
}

func NewTBinaryProtocol(t tthrift.TTransport, pool pool.BytesPool, strictRead, strictWrite bool) *TBinaryProtocol {
	p := &TBinaryProtocol{origTransport: t, strictRead: strictRead, strictWrite: strictWrite, bytesPool: pool}
	if et, ok := t.(tthrift.TRichTransport); ok {
		p.trans = et
	} else {
		p.trans = tthrift.NewTRichTransport(t)
	}
	p.reader = p.trans
	p.writer = p.trans
	return p
}

/**
* Writing Methods
 */

func (p *TBinaryProtocol) WriteMessageBegin(name string, typeId tthrift.TMessageType, seqId int32) error {
	if p.strictWrite {
		version := uint32(tthrift.VERSION_1) | uint32(typeId)
		e := p.WriteI32(int32(version))
		if e != nil {
			return e
		}
		e = p.WriteString(name)
		if e != nil {
			return e
		}
		e = p.WriteI32(seqId)
		return e
	} else {
		e := p.WriteString(name)
		if e != nil {
			return e
		}
		e = p.WriteByte(int8(typeId))
		if e != nil {
			return e
		}
		e = p.WriteI32(seqId)
		return e
	}
	return nil
}

func (p *TBinaryProtocol) WriteMessageEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteStructBegin(name string) error {
	return nil
}

func (p *TBinaryProtocol) WriteStructEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteFieldBegin(name string, typeId tthrift.TType, id int16) error {
	e := p.WriteByte(int8(typeId))
	if e != nil {
		return e
	}
	e = p.WriteI16(id)
	return e
}

func (p *TBinaryProtocol) WriteFieldEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteFieldStop() error {
	e := p.WriteByte(tthrift.STOP)
	return e
}

func (p *TBinaryProtocol) WriteMapBegin(keyType tthrift.TType, valueType tthrift.TType, size int) error {
	e := p.WriteByte(int8(keyType))
	if e != nil {
		return e
	}
	e = p.WriteByte(int8(valueType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *TBinaryProtocol) WriteMapEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteListBegin(elemType tthrift.TType, size int) error {
	e := p.WriteByte(int8(elemType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *TBinaryProtocol) WriteListEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteSetBegin(elemType tthrift.TType, size int) error {
	e := p.WriteByte(int8(elemType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *TBinaryProtocol) WriteSetEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteBool(value bool) error {
	if value {
		return p.WriteByte(1)
	}
	return p.WriteByte(0)
}

func (p *TBinaryProtocol) WriteByte(value int8) error {
	e := p.trans.WriteByte(byte(value))
	return tthrift.NewTProtocolException(e)
}

func (p *TBinaryProtocol) WriteI16(value int16) error {
	v := p.buffer[0:2]
	binary.BigEndian.PutUint16(v, uint16(value))
	_, e := p.writer.Write(v)
	return tthrift.NewTProtocolException(e)
}

func (p *TBinaryProtocol) WriteI32(value int32) error {
	v := p.buffer[0:4]
	binary.BigEndian.PutUint32(v, uint32(value))
	_, e := p.writer.Write(v)
	return tthrift.NewTProtocolException(e)
}

func (p *TBinaryProtocol) WriteI64(value int64) error {
	v := p.buffer[0:8]
	binary.BigEndian.PutUint64(v, uint64(value))
	_, err := p.writer.Write(v)
	return tthrift.NewTProtocolException(err)
}

func (p *TBinaryProtocol) WriteDouble(value float64) error {
	return p.WriteI64(int64(math.Float64bits(value)))
}

func (p *TBinaryProtocol) WriteString(value string) error {
	e := p.WriteI32(int32(len(value)))
	if e != nil {
		return e
	}
	_, err := p.trans.WriteString(value)
	return tthrift.NewTProtocolException(err)
}

func (p *TBinaryProtocol) WriteBinary(value []byte) error {
	e := p.WriteI32(int32(len(value)))
	if e != nil {
		return e
	}
	_, err := p.writer.Write(value)
	return tthrift.NewTProtocolException(err)
}

/**
* Reading methods
 */

func (p *TBinaryProtocol) ReadMessageBegin() (name string, typeId tthrift.TMessageType, seqId int32, err error) {
	size, e := p.ReadI32()
	if e != nil {
		return "", typeId, 0, tthrift.NewTProtocolException(e)
	}
	if size < 0 {
		typeId = tthrift.TMessageType(size & 0x0ff)
		version := int64(int64(size) & tthrift.VERSION_MASK)
		if version != tthrift.VERSION_1 {
			return name, typeId, seqId, tthrift.NewTProtocolExceptionWithType(tthrift.BAD_VERSION, fmt.Errorf("Bad version in ReadMessageBegin"))
		}
		name, e = p.ReadString()
		if e != nil {
			return name, typeId, seqId, tthrift.NewTProtocolException(e)
		}
		seqId, e = p.ReadI32()
		if e != nil {
			return name, typeId, seqId, tthrift.NewTProtocolException(e)
		}
		return name, typeId, seqId, nil
	}
	if p.strictRead {
		return name, typeId, seqId, tthrift.NewTProtocolExceptionWithType(tthrift.BAD_VERSION, fmt.Errorf("Missing version in ReadMessageBegin"))
	}
	name, e2 := p.readStringBody(size)
	if e2 != nil {
		return name, typeId, seqId, e2
	}
	b, e3 := p.ReadByte()
	if e3 != nil {
		return name, typeId, seqId, e3
	}
	typeId = tthrift.TMessageType(b)
	seqId, e4 := p.ReadI32()
	if e4 != nil {
		return name, typeId, seqId, e4
	}
	return name, typeId, seqId, nil
}

func (p *TBinaryProtocol) ReadMessageEnd() error {
	return nil
}

func (p *TBinaryProtocol) ReadStructBegin() (name string, err error) {
	return
}

func (p *TBinaryProtocol) ReadStructEnd() error {
	return nil
}

func (p *TBinaryProtocol) ReadFieldBegin() (name string, typeId tthrift.TType, seqId int16, err error) {
	t, err := p.ReadByte()
	typeId = tthrift.TType(t)
	if err != nil {
		return name, typeId, seqId, err
	}
	if t != tthrift.STOP {
		seqId, err = p.ReadI16()
	}
	return name, typeId, seqId, err
}

func (p *TBinaryProtocol) ReadFieldEnd() error {
	return nil
}

var invalidDataLength = tthrift.NewTProtocolExceptionWithType(tthrift.INVALID_DATA, errors.New("Invalid data length"))

func (p *TBinaryProtocol) ReadMapBegin() (kType, vType tthrift.TType, size int, err error) {
	k, e := p.ReadByte()
	if e != nil {
		err = tthrift.NewTProtocolException(e)
		return
	}
	kType = tthrift.TType(k)
	v, e := p.ReadByte()
	if e != nil {
		err = tthrift.NewTProtocolException(e)
		return
	}
	vType = tthrift.TType(v)
	size32, e := p.ReadI32()
	if e != nil {
		err = tthrift.NewTProtocolException(e)
		return
	}
	if size32 < 0 {
		err = invalidDataLength
		return
	}
	size = int(size32)
	return kType, vType, size, nil
}

func (p *TBinaryProtocol) ReadMapEnd() error {
	return nil
}

func (p *TBinaryProtocol) ReadListBegin() (elemType tthrift.TType, size int, err error) {
	b, e := p.ReadByte()
	if e != nil {
		err = tthrift.NewTProtocolException(e)
		return
	}
	elemType = tthrift.TType(b)
	size32, e := p.ReadI32()
	if e != nil {
		err = tthrift.NewTProtocolException(e)
		return
	}
	if size32 < 0 {
		err = invalidDataLength
		return
	}
	size = int(size32)

	return
}

func (p *TBinaryProtocol) ReadListEnd() error {
	return nil
}

func (p *TBinaryProtocol) ReadSetBegin() (elemType tthrift.TType, size int, err error) {
	b, e := p.ReadByte()
	if e != nil {
		err = tthrift.NewTProtocolException(e)
		return
	}
	elemType = tthrift.TType(b)
	size32, e := p.ReadI32()
	if e != nil {
		err = tthrift.NewTProtocolException(e)
		return
	}
	if size32 < 0 {
		err = invalidDataLength
		return
	}
	size = int(size32)
	return elemType, size, nil
}

func (p *TBinaryProtocol) ReadSetEnd() error {
	return nil
}

func (p *TBinaryProtocol) ReadBool() (bool, error) {
	b, e := p.ReadByte()
	v := true
	if b != 1 {
		v = false
	}
	return v, e
}

func (p *TBinaryProtocol) ReadByte() (int8, error) {
	v, err := p.trans.ReadByte()
	return int8(v), err
}

func (p *TBinaryProtocol) ReadI16() (value int16, err error) {
	buf := p.buffer[0:2]
	err = p.readAll(buf)
	value = int16(binary.BigEndian.Uint16(buf))
	return value, err
}

func (p *TBinaryProtocol) ReadI32() (value int32, err error) {
	buf := p.buffer[0:4]
	err = p.readAll(buf)
	value = int32(binary.BigEndian.Uint32(buf))
	return value, err
}

func (p *TBinaryProtocol) ReadI64() (value int64, err error) {
	buf := p.buffer[0:8]
	err = p.readAll(buf)
	value = int64(binary.BigEndian.Uint64(buf))
	return value, err
}

func (p *TBinaryProtocol) ReadDouble() (value float64, err error) {
	buf := p.buffer[0:8]
	err = p.readAll(buf)
	value = math.Float64frombits(binary.BigEndian.Uint64(buf))
	return value, err
}

func (p *TBinaryProtocol) ReadString() (value string, err error) {
	size, e := p.ReadI32()
	if e != nil {
		return "", e
	}
	if size < 0 {
		err = invalidDataLength
		return
	}

	return p.readStringBody(size)
}

func (p *TBinaryProtocol) ReadBinary() ([]byte, error) {
	size, e := p.ReadI32()
	if e != nil {
		return nil, e
	}
	if size < 0 {
		return nil, invalidDataLength
	}
	if uint64(size) > p.trans.RemainingBytes() {
		return nil, invalidDataLength
	}

	isize := int(size)
	buf := p.bytesPool.Get(isize)[:isize]
	_, err := io.ReadFull(p.trans, buf)
	return buf, tthrift.NewTProtocolException(err)
}

func (p *TBinaryProtocol) Flush() (err error) {
	return tthrift.NewTProtocolException(p.trans.Flush())
}

func (p *TBinaryProtocol) Skip(fieldType tthrift.TType) (err error) {
	return tthrift.SkipDefaultDepth(p, fieldType)
}

func (p *TBinaryProtocol) Transport() tthrift.TTransport {
	return p.origTransport
}

func (p *TBinaryProtocol) readAll(buf []byte) error {
	_, err := io.ReadFull(p.reader, buf)
	return tthrift.NewTProtocolException(err)
}

func (p *TBinaryProtocol) readStringBody(size int32) (value string, err error) {
	if size < 0 {
		return "", nil
	}
	if uint64(size) > p.trans.RemainingBytes() {
		return "", invalidDataLength
	}
	var buf []byte
	if int(size) <= len(p.buffer) {
		buf = p.buffer[0:size]
	} else {
		buf = make([]byte, size)
	}
	_, e := io.ReadFull(p.trans, buf)
	return string(buf), tthrift.NewTProtocolException(e)
}
