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
//go:generate sh -c "mockgen -package=build -destination=$GOPATH/src/$PACKAGE/build/mock_build.go -source=$GOPATH/src/$PACKAGE/build/types.go"
//go:generate sh -c "mockgen -package=fs -destination=$GOPATH/src/$PACKAGE/os/fs/mocks/mock_fs.go -source=$GOPATH/src/$PACKAGE/os/fs/types.go"
//go:generate sh -c "mockgen -package=exec -destination=$GOPATH/src/$PACKAGE/os/exec/mocks/mock_exec.go -source=$GOPATH/src/$PACKAGE/os/exec/types.go"

// mockgen rules for generating mocks for exported interfaces (reflection mode). TBH this mode is sketch af.
// (1) node package mocks
//go:generate sh -c "mockgen -package=node -destination=$GOPATH/src/$PACKAGE/node/mocks/mock_node.go github.com/m3db/m3em/node ServiceNode,Options"
//- delete the vendor prefix due to https://github.com/golang/mock/issues/30
//go:generate sed -i "" s@github.com/m3db/m3em/vendor/@@g $GOPATH/src/$PACKAGE/node/mocks/mock_node.go
// (2) m3em proto package mocks
//go:generate sh -c "mockgen -package=m3em -destination=$GOPATH/src/$PACKAGE/generated/proto/m3em/mock_m3em.go github.com/m3db/m3em/generated/proto/m3em OperatorClient,Operator_TransferClient"
//- delete the vendor prefix due to https://github.com/golang/mock/issues/30
//go:generate sed -i "" s@github.com/m3db/m3em/vendor/@@g $GOPATH/src/$PACKAGE/generated/proto/m3em/mock_m3em.go
//- mockgen creates a circle by importing the package within itself
//go:generate sed -i "" -e s@m3em\.@@g $GOPATH/src/$PACKAGE/generated/proto/m3em/mock_m3em.go
//go:generate sed -i "" s@.*m3em.*github.com.*@@g $GOPATH/src/$PACKAGE/generated/proto/m3em/mock_m3em.go

package generated
