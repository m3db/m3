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

package auth

import (
	"context"
	"net/http"
)

type keyType int

// AuthorizationType designates a type of authorization.
type AuthorizationType int

type errorResponseHandler func(w http.ResponseWriter, code int, msg string) error

const (
	// UserIDField is a key
	UserIDField keyType = iota
)

const (
	// UnknownAuthorization is the unknown authorizationType case.
	UnknownAuthorization AuthorizationType = iota
	// NoAuthorization is the no authorizationType case.
	NoAuthorization
	// ReadOnlyAuthorization is the read only authorizationType case.
	ReadOnlyAuthorization
	// WriteOnlyAuthorization is the write only authorizationType case.
	WriteOnlyAuthorization
	// ReadWriteAuthorization is the read and write authorizationType case.
	ReadWriteAuthorization
)

// HTTPAuthService defines how to handle requests for various http authentication and authorization methods.
type HTTPAuthService interface {
	// NewAuthHandler should return a handler that performs some check on the request coming into the given handler
	// and then runs the handler if it is. If the request passes authentication/authorization successfully, it should call SetUser
	// to make the callers id available to the service in a global context. errHandler should be passed in to properly format the
	// the error and respond the the request in the event of bad auth.
	NewAuthHandler(authType AuthorizationType, next http.Handler, errHandler errorResponseHandler) http.Handler

	// SetUser sets a userID that identifies the api caller in the global context.
	SetUser(parent context.Context, userID string) context.Context

	// GetUser fetches the ID of an api caller from the global context.
	GetUser(ctx context.Context) (string, error)
}
