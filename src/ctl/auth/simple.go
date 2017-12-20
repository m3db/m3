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
	"fmt"
	"net/http"
)

// SimpleAuthConfig holds this configuration necessary for a simple auth implementation.
type SimpleAuthConfig struct {
	Authentication authenticationConfig `yaml:"authentication"`
	Authorization  authorizationConfig  `yaml:"authorization"`
}

// authenticationConfig holds this configuration necessary for a simple authentication implementation.
type authenticationConfig struct {
	// This is an HTTP header that identifies the user performing the operation.
	UserIDHeader string `yaml:"userIDHeader" validate:"nonzero"`
	// This is an HTTP header that identifies the user originating the operation.
	OriginatorIDHeader string `yaml:"originatorIDHeader"`
}

// authorizationConfig holds this configuration necessary for a simple authorization implementation.
type authorizationConfig struct {
	// This indicates whether reads should use a read whitelist.
	ReadWhitelistEnabled bool `yaml:"readWhitelistEnabled,omitempty"`
	// This is a list of users that are allowed to perform read operations.
	ReadWhitelistedUserIDs []string `yaml:"readWhitelistedUserIDs,omitempty"`
	// This indicates whether writes should use a write whitelist.
	WriteWhitelistEnabled bool `yaml:"writeWhitelistEnabled,omitempty"`
	// This is a list of users that are allowed to perform write operations.
	WriteWhitelistedUserIDs []string `yaml:"writeWhitelistedUserIDs,omitempty"`
}

// NewSimpleAuth creates a new simple auth instance given using the provided config.
func (ac SimpleAuthConfig) NewSimpleAuth() HTTPAuthService {
	return simpleAuth{
		authentication: simpleAuthentication{
			userIDHeader:       ac.Authentication.UserIDHeader,
			originatorIDHeader: ac.Authentication.OriginatorIDHeader,
		},
		authorization: simpleAuthorization{
			readWhitelistEnabled:    ac.Authorization.ReadWhitelistEnabled,
			readWhitelistedUserIDs:  ac.Authorization.ReadWhitelistedUserIDs,
			writeWhitelistEnabled:   ac.Authorization.WriteWhitelistEnabled,
			writeWhitelistedUserIDs: ac.Authorization.WriteWhitelistedUserIDs,
		},
	}
}

type simpleAuth struct {
	authentication simpleAuthentication
	authorization  simpleAuthorization
}

type simpleAuthentication struct {
	userIDHeader       string
	originatorIDHeader string
}

func (a simpleAuthentication) authenticate(userID string) error {
	if userID == "" {
		return fmt.Errorf("must provide header: [%s]", a.userIDHeader)
	}
	return nil
}

type simpleAuthorization struct {
	readWhitelistEnabled    bool
	readWhitelistedUserIDs  []string
	writeWhitelistEnabled   bool
	writeWhitelistedUserIDs []string
}

func (a simpleAuthorization) authorize(httpMethod, userID string) error {
	switch httpMethod {
	case http.MethodGet:
		return a.authorizeUser(a.readWhitelistEnabled, a.readWhitelistedUserIDs, userID)
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return a.authorizeUser(a.writeWhitelistEnabled, a.writeWhitelistedUserIDs, userID)
	default:
		return fmt.Errorf("unsupported request method: %s", httpMethod)
	}
}

func (a simpleAuthorization) authorizeUser(useWhitelist bool, whitelistedUsers []string, userID string) error {
	if !useWhitelist {
		return nil
	}

	for _, u := range whitelistedUsers {
		if u == userID {
			return nil
		}
	}
	return fmt.Errorf("supplied userID: [%s] is not authorized", userID)
}

// Authenticate looks for a header defining a user name. If it finds it, runs the actual http handler passed as a parameter.
// Otherwise, it returns an Unauthorized http response.
func (a simpleAuth) NewAuthHandler(next http.Handler, errHandler errorResponseHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			userID       = r.Header.Get(a.authentication.userIDHeader)
			originatorID = r.Header.Get(a.authentication.originatorIDHeader)
		)
		if originatorID == "" {
			originatorID = userID
		}
		err := a.authentication.authenticate(originatorID)
		if err != nil {
			errHandler(w, http.StatusUnauthorized, err.Error())
			return
		}

		err = a.authorization.authorize(r.Method, userID)
		if err != nil {
			errHandler(w, http.StatusForbidden, err.Error())
			return
		}

		ctx := a.SetUser(r.Context(), originatorID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// SetUser sets the user making the changes to the api.
func (a simpleAuth) SetUser(parent context.Context, userID string) context.Context {
	return context.WithValue(parent, UserIDField, userID)
}

// GetUser fetches the ID of an api caller from the global context.
func (a simpleAuth) GetUser(ctx context.Context) (string, error) {
	id := ctx.Value(UserIDField)
	if id == nil {
		return "", fmt.Errorf("couldn't identify user")
	}
	return id.(string), nil
}
