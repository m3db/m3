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
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

var (
	testUser               = "testUser"
	testOriginator         = "testOriginator"
	testUserIDHeader       = "testUserIDHeader"
	testOriginatorIDHeader = "testOriginatorIDHeader"
	testConfig             = SimpleAuthConfig{
		Authentication: authenticationConfig{
			UserIDHeader: "testHeader",
		},
		Authorization: authorizationConfig{
			ReadWhitelistEnabled:    true,
			WriteWhitelistEnabled:   false,
			ReadWhitelistedUserIDs:  []string{testUser},
			WriteWhitelistedUserIDs: []string{},
		},
	}
	testConfigWithOriginatorID = SimpleAuthConfig{
		Authentication: authenticationConfig{
			UserIDHeader:       testUserIDHeader,
			OriginatorIDHeader: testOriginatorIDHeader,
		},
		Authorization: authorizationConfig{
			ReadWhitelistEnabled:    true,
			WriteWhitelistEnabled:   true,
			ReadWhitelistedUserIDs:  []string{},
			WriteWhitelistedUserIDs: []string{testUser},
		},
	}
)

func TestSimpleAuthConfigUnmarshal(t *testing.T) {
	configStr := `
authentication:
  userIDHeader: user-id
authorization:
  readWhitelistEnabled: true
  readWhitelistedUserIDs:
    - foo
    - bar
  writeWhitelistEnabled: true
  writeWhitelistedUserIDs:
    - bar
    - baz
`
	var cfg SimpleAuthConfig
	require.NoError(t, yaml.Unmarshal([]byte(configStr), &cfg))
	require.Equal(t, "user-id", cfg.Authentication.UserIDHeader)
	require.True(t, cfg.Authorization.ReadWhitelistEnabled)
	require.Equal(t, []string{"foo", "bar"}, cfg.Authorization.ReadWhitelistedUserIDs)
	require.True(t, cfg.Authorization.WriteWhitelistEnabled)
	require.Equal(t, []string{"bar", "baz"}, cfg.Authorization.WriteWhitelistedUserIDs)
}

func TestNewSimpleAuth(t *testing.T) {
	an := testConfig.NewSimpleAuth().(simpleAuth).authentication
	az := testConfig.NewSimpleAuth().(simpleAuth).authorization
	require.Equal(t, an.userIDHeader, "testHeader")
	require.Equal(t, az.readWhitelistEnabled, true)
	require.Equal(t, az.writeWhitelistEnabled, false)
	require.Equal(t, az.readWhitelistedUserIDs, []string{"testUser"})
	require.Equal(t, az.writeWhitelistedUserIDs, []string{})
}

func TestSetUser(t *testing.T) {
	a := testConfig.NewSimpleAuth()
	ctx := context.Background()
	require.Nil(t, ctx.Value(UserIDField))
	ctx = a.SetUser(ctx, "foo")
	require.Equal(t, "foo", ctx.Value(UserIDField).(string))
}

func TestGetUser(t *testing.T) {
	a := testConfig.NewSimpleAuth()
	ctx := context.Background()

	id, err := a.GetUser(ctx)
	require.Empty(t, id)
	require.Error(t, err)

	ctx = a.SetUser(ctx, "foo")
	id, err = a.GetUser(ctx)
	require.Equal(t, "foo", id)
	require.NoError(t, err)
}

func TestSimpleAuthenticationAuthenticate(t *testing.T) {
	authentication := simpleAuthentication{
		userIDHeader: "foo",
	}

	require.Nil(t, authentication.authenticate("bar"))
	require.EqualError(t, authentication.authenticate(""), "must provide header: [foo]")
}

func TestSimpleAuthorizationAuthorize(t *testing.T) {
	authorization := simpleAuthorization{
		readWhitelistEnabled:    true,
		writeWhitelistEnabled:   false,
		readWhitelistedUserIDs:  []string{"foo", "bar"},
		writeWhitelistedUserIDs: []string{"foo", "bar"},
	}

	require.Nil(t, authorization.authorize("GET", "foo"))
	require.Nil(t, authorization.authorize("POST", "foo"))
	require.EqualError(t, authorization.authorize("OPTIONS", "foo"), "unsupported request method: OPTIONS")
	require.EqualError(t, authorization.authorize("GET", "baz"), "supplied userID: [baz] is not authorized")
}

func TestHealthCheck(t *testing.T) {
	a := testConfig.NewSimpleAuth()
	f := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, err := a.GetUser(r.Context())
		require.NoError(t, err)
		require.Equal(t, "testHeader", v)
	})

	wrappedCall := a.NewAuthHandler(f, writeAPIResponse)
	wrappedCall.ServeHTTP(httptest.NewRecorder(), &http.Request{})
}

func TestAuthenticateFailure(t *testing.T) {
	a := testConfig.NewSimpleAuth()
	f := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, err := a.GetUser(r.Context())
		require.NoError(t, err)
		require.Equal(t, "testHeader", v)
	})
	recorder := httptest.NewRecorder()

	wrappedCall := a.NewAuthHandler(f, writeAPIResponse)
	wrappedCall.ServeHTTP(recorder, &http.Request{})
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.Equal(t, "application/json", recorder.HeaderMap["Content-Type"][0])
}

func TestAuthenticateWithOriginatorID(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "/update", nil)
	require.NoError(t, err)
	req.Header.Add(testUserIDHeader, testUser)
	req.Header.Add(testOriginatorIDHeader, testOriginator)

	a := testConfigWithOriginatorID.NewSimpleAuth()
	f := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, err := a.GetUser(r.Context())
		require.NoError(t, err)
		require.Equal(t, testOriginator, v)
		writeAPIResponse(w, http.StatusOK, "success!")
	})
	recorder := httptest.NewRecorder()
	wrappedCall := a.NewAuthHandler(f, writeAPIResponse)
	wrappedCall.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "application/json", recorder.HeaderMap["Content-Type"][0])
}

func TestAuthorizeFailure(t *testing.T) {
	a := testConfig.NewSimpleAuth()
	f := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, err := a.GetUser(r.Context())
		require.NoError(t, err)
		require.Equal(t, "testHeader", v)
	})
	recorder := httptest.NewRecorder()
	req, err := http.NewRequest("Get", "/create", bytes.NewBuffer(nil))
	require.NoError(t, err)
	req.Header.Add("testHeader", "validUserID")

	wrappedCall := a.NewAuthHandler(f, writeAPIResponse)
	wrappedCall.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.Equal(t, "application/json", recorder.HeaderMap["Content-Type"][0])
}

func writeAPIResponse(w http.ResponseWriter, code int, msg string) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, err := w.Write([]byte(msg))

	return err
}
