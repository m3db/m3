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

package rules

import (
	"errors"
	"sort"

	"github.com/m3db/m3metrics/generated/proto/rulepb"
	"github.com/m3db/m3metrics/pipeline"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/view"
	"github.com/m3db/m3metrics/x/bytes"
)

var (
	emptyRollupTarget rollupTarget

	errNilRollupTargetV1Proto = errors.New("nil rollup target v1 proto")
	errNilRollupTargetV2Proto = errors.New("nil rollup target v2 proto")
)

// rollupTarget dictates how to roll up metrics. Metrics associated with a rollup
// target will be rolled up as dictated by the operations in the pipeline, and stored
// under the provided storage policies.
type rollupTarget struct {
	Pipeline        pipeline.Pipeline
	StoragePolicies policy.StoragePolicies
}

// newRollupTargetFromV1Proto creates a new rollup target from v1 proto
// for backward compatibility purposes.
func newRollupTargetFromV1Proto(pb *rulepb.RollupTarget) (rollupTarget, error) {
	if pb == nil {
		return emptyRollupTarget, errNilRollupTargetV1Proto
	}
	aggregationID, storagePolicies, err := toAggregationIDAndStoragePolicies(pb.Policies)
	if err != nil {
		return emptyRollupTarget, err
	}
	tags := make([]string, len(pb.Tags))
	copy(tags, pb.Tags)
	sort.Strings(tags)
	rollupOp := pipeline.OpUnion{
		Type: pipeline.RollupOpType,
		Rollup: pipeline.RollupOp{
			NewName:       []byte(pb.Name),
			Tags:          bytes.ArraysFromStringArray(tags),
			AggregationID: aggregationID,
		},
	}
	pipeline := pipeline.NewPipeline([]pipeline.OpUnion{rollupOp})
	return rollupTarget{
		Pipeline:        pipeline,
		StoragePolicies: storagePolicies,
	}, nil
}

// newRollupTargetFromProto creates a new rollup target from v2 proto.
func newRollupTargetFromV2Proto(pb *rulepb.RollupTargetV2) (rollupTarget, error) {
	if pb == nil {
		return emptyRollupTarget, errNilRollupTargetV2Proto
	}
	pipeline, err := pipeline.NewPipelineFromProto(pb.Pipeline)
	if err != nil {
		return emptyRollupTarget, err
	}
	storagePolicies, err := policy.NewStoragePoliciesFromProto(pb.StoragePolicies)
	if err != nil {
		return emptyRollupTarget, err
	}
	return rollupTarget{
		Pipeline:        pipeline,
		StoragePolicies: storagePolicies,
	}, nil
}

func newRollupTargetFromView(rtv view.RollupTarget) rollupTarget {
	return rollupTarget{
		Pipeline:        rtv.Pipeline,
		StoragePolicies: rtv.StoragePolicies,
	}
}

func (t rollupTarget) rollupTargetView() view.RollupTarget {
	return view.RollupTarget{
		Pipeline:        t.Pipeline,
		StoragePolicies: t.StoragePolicies,
	}
}

// clone clones a rollup target.
func (t *rollupTarget) clone() rollupTarget {
	return rollupTarget{
		Pipeline:        t.Pipeline.Clone(),
		StoragePolicies: t.StoragePolicies.Clone(),
	}
}

// proto returns the proto representation of a rollup target.
func (t *rollupTarget) proto() (*rulepb.RollupTargetV2, error) {
	pipeline, err := t.Pipeline.Proto()
	if err != nil {
		return nil, err
	}
	storagePolicies, err := t.StoragePolicies.Proto()
	if err != nil {
		return nil, err
	}
	return &rulepb.RollupTargetV2{
		Pipeline:        pipeline,
		StoragePolicies: storagePolicies,
	}, nil
}

func newRollupTargetsFromView(targets []view.RollupTarget) []rollupTarget {
	res := make([]rollupTarget, 0, len(targets))
	for _, t := range targets {
		res = append(res, newRollupTargetFromView(t))
	}
	return res
}
