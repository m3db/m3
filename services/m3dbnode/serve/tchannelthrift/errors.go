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

package tchannelthrift

import (
	"fmt"

	"github.com/m3db/m3db/services/m3dbnode/serve/tchannelthrift/thrift/gen-go/rpc"
)

func newNodeError(errType rpc.NodeErrorType, err error) *rpc.NodeError {
	nodeErr := rpc.NewNodeError()
	nodeErr.Type = errType
	nodeErr.Message = fmt.Sprintf("%v", err)
	return nodeErr
}

func newNodeInternalError(err error) *rpc.NodeError {
	return newNodeError(rpc.NodeErrorType_INTERNAL_ERROR, err)
}

func newNodeBadRequestError(err error) *rpc.NodeError {
	return newNodeError(rpc.NodeErrorType_BAD_REQUEST, err)
}

func newWriteError(err error) *rpc.WriteError {
	writeErr := rpc.NewWriteError()
	writeErr.Message = fmt.Sprintf("%v", err)
	return writeErr
}

func newBadRequestWriteError(err error) *rpc.WriteError {
	writeErr := rpc.NewWriteError()
	writeErr.Type = rpc.NodeErrorType_BAD_REQUEST
	writeErr.Message = fmt.Sprintf("%v", err)
	return writeErr
}

func newWriteBatchError(index int, err error) *rpc.WriteBatchError {
	batchErr := rpc.NewWriteBatchError()
	batchErr.ElementErrorIndex = int64(index)
	batchErr.Error = newWriteError(err)
	return batchErr
}

func newBadRequestWriteBatchError(index int, err error) *rpc.WriteBatchError {
	batchErr := rpc.NewWriteBatchError()
	batchErr.ElementErrorIndex = int64(index)
	batchErr.Error = newBadRequestWriteError(err)
	return batchErr
}
