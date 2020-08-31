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
//go:generate sh -c "mockgen -package=aggregator github.com/m3db/m3/src/aggregator/aggregator Aggregator,ElectionManager,FlushTimesManager,PlacementManager | genclean -pkg github.com/m3db/m3/src/aggregator/aggregator -out $GOPATH/src/github.com/m3db/m3/src/aggregator/aggregator/aggregator_mock.go"
//go:generate sh -c "mockgen -package=client github.com/m3db/m3/src/aggregator/client Client,AdminClient | genclean -pkg github.com/m3db/m3/src/aggregator/client -out $GOPATH/src/github.com/m3db/m3/src/aggregator/client/client_mock.go"
//go:generate sh -c "mockgen -package=handler github.com/m3db/m3/src/aggregator/aggregator/handler Handler | genclean -pkg github.com/m3db/m3/src/aggregator/aggregator/handler -out $GOPATH/src/github.com/m3db/m3/src/aggregator/aggregator/handler/handler_mock.go"
//go:generate sh -c "mockgen -package=runtime github.com/m3db/m3/src/aggregator/runtime OptionsWatcher | genclean -pkg github.com/m3db/m3/src/aggregator/runtime -out $GOPATH/src/github.com/m3db/m3/src/aggregator/runtime/runtime_mock.go"

// mockgen rules for generating mocks for unexported interfaces (file mode).
//go:generate sh -c "mockgen -package=aggregator -destination=$GOPATH/src/github.com/m3db/m3/src/aggregator/aggregator/flush_mgr_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/aggregator/aggregator/flush_mgr.go"
//go:generate sh -c "mockgen -package=aggregator -destination=$GOPATH/src/github.com/m3db/m3/src/aggregator/aggregator/flush_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/aggregator/aggregator/flush.go"
//go:generate sh -c "mockgen -package=client -destination=$GOPATH/src/github.com/m3db/m3/src/aggregator/client/writer_mgr_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/aggregator/client/writer_mgr.go"
//go:generate sh -c "mockgen -package=client -destination=$GOPATH/src/github.com/m3db/m3/src/aggregator/client/writer_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/aggregator/client/writer.go"
//go:generate sh -c "mockgen -package=client -destination=$GOPATH/src/github.com/m3db/m3/src/aggregator/client/queue_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/aggregator/client/queue.go"
//go:generate sh -c "mockgen -package=deploy -destination=$GOPATH/src/github.com/m3db/m3/src/aggregator/tools/deploy/client_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/aggregator/tools/deploy/client.go"
//go:generate sh -c "mockgen -package=deploy -destination=$GOPATH/src/github.com/m3db/m3/src/aggregator/tools/deploy/manager_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/aggregator/tools/deploy/manager.go"
//go:generate sh -c "mockgen -package=deploy -destination=$GOPATH/src/github.com/m3db/m3/src/aggregator/tools/deploy/planner_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/aggregator/tools/deploy/planner.go"
//go:generate sh -c "mockgen -package=deploy -destination=$GOPATH/src/github.com/m3db/m3/src/aggregator/tools/deploy/validator_mock.go -source=$GOPATH/src/github.com/m3db/m3/src/aggregator/tools/deploy/validator.go"

package mocks
