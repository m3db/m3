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
	"fmt"
	"strings"
	"testing"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/errors"
	"github.com/m3db/m3/src/dbnode/topology"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/stretchr/testify/assert"
	"github.com/uber/tchannel-go"
)

func TestConsistencyResultError(t *testing.T) {
	badReqErr := xerrors.NewRenamedError(&rpc.Error{
		Type: rpc.ErrorType_BAD_REQUEST,
	}, fmt.Errorf("renamed error"))
	timeoutErr := errors.NewTimeoutError(fmt.Errorf("timeout"))

	level := topology.ReadConsistencyLevelMajority
	enqueued := 4
	responded := 4
	errs := []error{fmt.Errorf("another error"), timeoutErr, badReqErr} // badReqErr takes precedence

	err := error(newConsistencyResultError(level, enqueued, responded, errs))

	assert.True(t, strings.HasPrefix(err.Error(),
		"failed to meet consistency level majority with 1/4 success, 4 nodes responded, errors:"))
	assert.Equal(t, badReqErr, xerrors.InnerError(err))
	assert.True(t, IsBadRequestError(err))
	assert.Equal(t, 4, NumResponded(err))
	assert.Equal(t, 1, NumSuccess(err))
	assert.Equal(t, 3, NumError(err))
}

func TestConsistencyResultTimeoutError(t *testing.T) {
	timeoutErr := errors.NewTimeoutError(fmt.Errorf("timeout"))

	level := topology.ReadConsistencyLevelMajority
	enqueued := 3
	responded := 3
	errs := []error{fmt.Errorf("another error"), timeoutErr}

	err := error(newConsistencyResultError(level, enqueued, responded, errs))

	assert.True(t, strings.HasPrefix(err.Error(),
		"failed to meet consistency level majority with 1/3 success, 3 nodes responded, errors:"))
	assert.Equal(t, timeoutErr, xerrors.InnerError(err))
	assert.True(t, IsTimeoutError(err))
	assert.Equal(t, 3, NumResponded(err))
	assert.Equal(t, 1, NumSuccess(err))
	assert.Equal(t, 2, NumError(err))
}

func TestConsistencyResultTchannelTimeoutError(t *testing.T) {
	timeoutErr := xerrors.NewRenamedError(tchannel.ErrTimeout, fmt.Errorf("error"))

	level := topology.ReadConsistencyLevelMajority
	enqueued := 3
	responded := 3
	errs := []error{fmt.Errorf("another error"), timeoutErr}

	err := error(newConsistencyResultError(level, enqueued, responded, errs))

	assert.True(t, strings.HasPrefix(err.Error(),
		"failed to meet consistency level majority with 1/3 success, 3 nodes responded, errors:"))
	assert.Equal(t, timeoutErr, xerrors.InnerError(err))
	assert.True(t, IsTimeoutError(err))
	assert.Equal(t, 3, NumResponded(err))
	assert.Equal(t, 1, NumSuccess(err))
	assert.Equal(t, 2, NumError(err))
}
