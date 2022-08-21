// Copyright (c) 2017 Uber Technologies, Inc.
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

// mockgen rules for generating mocks for unexported interfaces (file mode)
//go:generate sh -c "mockgen -package=build -destination=../../build/build_mock.go -source=../../build/types.go"
//go:generate sh -c "mockgen -package=fs -destination=../../os/fs/mocks/fs_mock.go -source=../../os/fs/types.go"
//go:generate sh -c "mockgen -package=exec -destination=../../os/exec/mocks/exec_mock.go -source=../../os/exec/types.go"

// mockgen rules for generating mocks for exported interfaces (reflection mode)
//go:generate sh -c "mockgen -package=node github.com/m3db/m3/src/m3em/node ServiceNode,Options | genclean -pkg github.com/m3db/m3/src/m3em/node -out ../../node/node_mock.go"
//go:generate sh -c "mockgen -package=m3em github.com/m3db/m3/src/m3em/generated/proto/m3em OperatorClient,Operator_PushFileClient,Operator_PullFileClient,Operator_PullFileServer | genclean -pkg github.com/m3db/m3/src/m3em/generated/proto/m3em -out ../../generated/proto/m3em/m3em_mock.go"

package generated
