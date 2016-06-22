// Copyright (c) 2016 Uber Technologies, Inc.
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

package errors

import (
	"fmt"

	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
)

func newError(errType rpc.ErrorType, err error) *rpc.Error {
	rpcErr := rpc.NewError()
	rpcErr.Type = errType
	rpcErr.Message = fmt.Sprintf("%v", err)
	return rpcErr
}

// NewInternalError creates a new internal error
func NewInternalError(err error) *rpc.Error {
	return newError(rpc.ErrorType_INTERNAL_ERROR, err)
}

// NewBadRequestError creates a new bad request error
func NewBadRequestError(err error) *rpc.Error {
	return newError(rpc.ErrorType_BAD_REQUEST, err)
}

// NewWriteError creates a new write error
func NewWriteError(err error) *rpc.WriteError {
	writeErr := rpc.NewWriteError()
	writeErr.Message = fmt.Sprintf("%v", err)
	return writeErr
}

// NewBadRequestWriteError creates a new bad request write error
func NewBadRequestWriteError(err error) *rpc.WriteError {
	writeErr := rpc.NewWriteError()
	writeErr.Type = rpc.ErrorType_BAD_REQUEST
	writeErr.Message = fmt.Sprintf("%v", err)
	return writeErr
}

// NewWriteBatchError creates a new write batch error
func NewWriteBatchError(index int, err error) *rpc.WriteBatchError {
	batchErr := rpc.NewWriteBatchError()
	batchErr.ElementErrorIndex = int64(index)
	batchErr.Error = NewWriteError(err)
	return batchErr
}

// NewBadRequestWriteBatchError creates a new bad request write batch error
func NewBadRequestWriteBatchError(index int, err error) *rpc.WriteBatchError {
	batchErr := rpc.NewWriteBatchError()
	batchErr.ElementErrorIndex = int64(index)
	batchErr.Error = NewBadRequestWriteError(err)
	return batchErr
}
