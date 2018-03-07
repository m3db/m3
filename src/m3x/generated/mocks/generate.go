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

//go:generate sh -c "mockgen -package=ident github.com/m3db/m3x/ident ID,TagIterator | mockclean -pkg github.com/m3db/m3x/ident -out $GOPATH/src/github.com/m3db/m3x/ident/ident_mock.go"
//go:generate sh -c "mockgen -package=checked github.com/m3db/m3x/checked Bytes | mockclean -pkg github.com/m3db/m3x/checked -out $GOPATH/src/github.com/m3db/m3x/checked/checked_mock.go"
//go:generate sh -c "mockgen -package=pool github.com/m3db/m3x/pool CheckedBytesPool | mockclean -pkg github.com/m3db/m3x/pool -out $GOPATH/src/github.com/m3db/m3x/pool/pool_mock.go"

package generated
