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

package errors

import (
	"errors"
)

var (
	// ErrNilWriteQuery is returned when trying to write a nil query.
	ErrNilWriteQuery = errors.New("nil write query")

	// ErrRemoteWriteQuery is returned when trying to write to a remote
	// endpoint query.
	ErrRemoteWriteQuery = errors.New("cannot write to remote endpoint")

	// ErrNotImplemented is returned when the storage endpoint is not implemented.
	ErrNotImplemented = errors.New("not implemented")

	// ErrInvalidFetchResponse is returned when fetch fails from storage.
	ErrInvalidFetchResponse = errors.New("invalid response from fetch")

	// ErrFetchResponseOrder is returned fetch responses are not in order.
	ErrFetchResponseOrder = errors.New("responses out of order for fetch")

	// ErrFetchRequestType is an error returned when response from fetch has
	// an invalid type.
	ErrFetchRequestType = errors.New("invalid request type")

	// ErrNoValidResults is an error returned when there are no stores
	// that succeeded the fanout.
	ErrNoValidResults = errors.New("no valid results in fanout")

	// ErrInvalidFetchResult is an error returned when fetch result is invalid.
	ErrInvalidFetchResult = errors.New("invalid fetch result")

	// ErrZeroInterval is an error returned when fetch interval is 0.
	ErrZeroInterval = errors.New("interval cannot be 0")

	// ErrInvalidOperation is an error returned when fetch raw is called on wrong
	// storage type.
	ErrInvalidOperation = errors.New("can only fetch raw iterators on" +
		" local storage")

	// ErrBadRequestType is an error returned when a request type is unexpected.
	ErrBadRequestType = errors.New("request is an invalid type")

	// ErrCannotDecodeCompressedTags is an error returned when compressed
	// tags cannot be decoded.
	ErrCannotDecodeCompressedTags = errors.New("unable to decode compressed tags")

	// ErrCannotDecodeDecompressedTags is an error returned when decompressed
	// tags cannot be decoded.
	ErrCannotDecodeDecompressedTags = errors.New("unable to decode" +
		" decompressed tags")

	// ErrCannotEncodeCompressedTags is an error returned when compressed tags
	// cannot be encoded.
	ErrCannotEncodeCompressedTags = errors.New("unable to encode compressed tags")

	// ErrOnlyFixedResSupported is an error returned we try to get step size
	// for variable resolution.
	ErrOnlyFixedResSupported = errors.New("only fixed resolution supported")

	// ErrUnexpectedGRPCResponseType is an error returned when rpc response type
	// is unhandled.
	ErrUnexpectedGRPCResponseType = errors.New("unexpected grpc response type")

	// ErrInconsistentCompleteTagsType is an error returned when a complete tags
	// request returns an inconsistenent type.
	ErrInconsistentCompleteTagsType = errors.New("inconsistent complete tags" +
		" response type")
)
