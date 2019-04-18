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

// mockgen rules for generating mocks for exported interfaces (reflection mode).
//go:generate sh -c "mockgen -package=downsample $PACKAGE/src/cmd/services/m3coordinator/downsample Downsampler,MetricsAppender,SamplesAppender | genclean -pkg $PACKAGE/src/cmd/services/m3coordinator/downsample -out $GOPATH/src/$PACKAGE/src/cmd/services/m3coordinator/downsample/downsample_mock.go"
//go:generate sh -c "mockgen -package=storage -destination=$GOPATH/src/$PACKAGE/src/query/storage/storage_mock.go $PACKAGE/src/query/storage Storage"
//go:generate sh -c "mockgen -package=block -destination=$GOPATH/src/$PACKAGE/src/query/block/block_mock.go $PACKAGE/src/query/block Block,StepIter,SeriesIter,Builder,Step"
//go:generate sh -c "mockgen -package=ingest -destination=$GOPATH/src/$PACKAGE/src/cmd/services/m3coordinator/ingest/write_mock.go $PACKAGE/src/cmd/services/m3coordinator/ingest DownsamplerAndWriter"
//go:generate sh -c "mockgen -package=transform -destination=$GOPATH/src/$PACKAGE/src/query/executor/transform/types_mock.go $PACKAGE/src/query/executor/transform OpNode"

// mockgen rules for generating mocks for unexported interfaces (file mode).
//go:generate sh -c "mockgen -package=m3ql -destination=$GOPATH/src/github.com/m3db/m3/src/query/parser/m3ql/types_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/query/parser/m3ql/types.go"
//go:generate sh -c "mockgen -package=transform -destination=$GOPATH/src/github.com/m3db/m3/src/query/executor/transform/exec_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/query/executor/transform/exec.go"
//go:generate sh -c "mockgen -package=temporal -destination=$GOPATH/src/github.com/m3db/m3/src/query/functions/temporal/dependencies_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/query/functions/temporal/dependencies.go" controller
//go:generate sh -c "mockgen -package=cost -destination=$GOPATH/src/github.com/m3db/m3/src/query/cost/cost_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/query/cost/cost.go"

package mocks
