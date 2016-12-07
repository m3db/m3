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

package client

import (
	"time"

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/ts"

	xlog "github.com/m3db/m3x/log"
)

func validateBlock(
	resultBlock *rpc.Block,
	batchBlock blockMetadata,
	blocksResult *blocksResult,
	earliestBlockStart time.Time,
	id ts.ID,
	host string,
	log xlog.Logger,
) error {
	if resultBlock.Start != batchBlock.start.UnixNano() {
		// If fell out of retention during request this is healthy, otherwise an error
		if !time.Unix(0, resultBlock.Start).Before(earliestBlockStart) {
			log.WithFields(
				xlog.NewLogField("id", id.String()),
				xlog.NewLogField("expectedStart", batchBlock.start.UnixNano()),
				xlog.NewLogField("actualStart", resultBlock.Start),
			).Errorf("stream blocks response from peer %s returned mismatched block start", host)
		}
		return errBlockStartMismatch
	}

	if resultBlock.Err != nil {
		log.WithFields(
			xlog.NewLogField("id", id.String()),
			xlog.NewLogField("start", resultBlock.Start),
			xlog.NewLogField("errorType", resultBlock.Err.Type),
			xlog.NewLogField("errorMessage", resultBlock.Err.Message),
		).Errorf("stream blocks response from peer %s returned block error", host)
		return resultBlock.Err
	}

	if err := blocksResult.addBlockFromPeer(id, resultBlock); err != nil {
		log.WithFields(
			xlog.NewLogField("id", id.String()),
			xlog.NewLogField("start", resultBlock.Start),
			xlog.NewLogField("error", err),
		).Errorf("stream blocks response from peer %s bad block response", host)
		return err
	}

	return nil
}
