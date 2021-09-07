// Copyright (c) 2021 Uber Technologies, Inc.
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

package metricpb

import (
	"testing"

	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
)

func TestCatchNewFields(t *testing.T) {
	verifyFields(t, &StagedMetadatas{}, 1)
	verifyFields(t, &StagedMetadata{}, 3)
	verifyFields(t, &Metadata{}, 1)
	verifyFields(t, &PipelineMetadata{}, 5)
	verifyFields(t, &pipelinepb.AppliedPipeline{}, 1)
	verifyFields(t, &pipelinepb.AppliedPipelineOp{}, 3)
	verifyFields(t, &pipelinepb.AppliedRollupOp{}, 2)
}

func verifyFields(t *testing.T, m descriptor.Message, expectedFieldCount int) {
	_, d := descriptor.ForMessage(m)
	require.Len(t, d.Field, expectedFieldCount, "Unexpected number of fields for %s. "+
		"If you added a new field you need to update the reuse() function in custom_unmarshal.go to reset the field "+
		"for reuse. After that you can update the expected number of fields in this test.", *d.Name)
}
