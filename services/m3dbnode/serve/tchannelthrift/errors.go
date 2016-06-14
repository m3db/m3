package tchannelthrift

import (
	"fmt"

	"code.uber.internal/infra/memtsdb/services/m3dbnode/serve/tchannelthrift/thrift/gen-go/rpc"
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
