//go:build integration
// +build integration

// Copyright (c) 2020 Uber Technologies, Inc.
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

package integration

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestEncoderLimit(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// We don't want a tick to happen during this test, since that will
	// interfere with testing encoders due to the tick merging them.
	testOpts := NewTestOptions(t).SetTickMinimumInterval(time.Minute)
	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	require.NoError(t, testSetup.StartServer())
	log.Info("server is now up")

	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Info("server is now down")
	}()

	now := testSetup.NowFn()()

	db := testSetup.DB()
	mgr := db.Options().RuntimeOptionsManager()
	encoderLimit := 5
	newRuntimeOpts := mgr.Get().SetEncodersPerBlockLimit(encoderLimit)
	mgr.Update(newRuntimeOpts)

	session, err := testSetup.M3DBClient().DefaultSession()
	require.NoError(t, err)
	nsID := testNamespaces[0]
	seriesID := ident.StringID("foo")

	for i := 0; i < encoderLimit+5; i++ {
		err = session.Write(
			nsID, seriesID,
			// Write backwards so that a new encoder gets created every write.
			now.Add(time.Duration(50-i)*time.Second),
			123, xtime.Second, nil,
		)

		if i >= encoderLimit {
			require.Error(t, err)
			// A rejected write due to hitting the max encoder limit should be
			// a bad request so that the client knows to not repeat the write
			// request, since that will exacerbate the problem.
			require.True(t, client.IsBadRequestError(err))
		} else {
			require.NoError(t, err)
		}
	}

	for i := 0; i < 10; i++ {
		err = session.Write(
			nsID, seriesID,
			now.Add(time.Duration(51+i)*time.Second),
			123, xtime.Second, nil,
		)

		// Even though we're doing more writes, these can fit into existing
		// encoders since they are all ahead of existing writes, so expect
		// no errors writing.
		require.NoError(t, err)
	}

	// Now allow an unlimited number of encoders.
	encoderLimit = 0
	newRuntimeOpts = mgr.Get().SetEncodersPerBlockLimit(encoderLimit)
	mgr.Update(newRuntimeOpts)

	for i := 0; i < 20; i++ {
		err = session.Write(
			nsID, seriesID,
			now.Add(time.Duration(20-i)*time.Second),
			123, xtime.Second, nil,
		)

		// Now there's no encoder limit, so no error even though each of these
		// additional writes creates a new encoder.
		require.NoError(t, err)
	}
}
