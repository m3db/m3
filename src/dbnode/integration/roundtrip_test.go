// Copyright (c) 2016 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/testdata/prototest"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type setTestOptions func(t *testing.T, testOpts TestOptions) TestOptions

func TestRoundtrip(t *testing.T) {
	testRoundtrip(t, nil, nil)
}

func TestProtoRoundtrip(t *testing.T) {
	testRoundtrip(t, setProtoTestOptions, setProtoTestInputConfig)
}

func setProtoTestInputConfig(inputData []generate.BlockConfig) {
	for i := 0; i < len(inputData); i++ {
		inputData[i].AnnGen = testProtoIter
	}
}

func setProtoTestOptions(t *testing.T, testOpts TestOptions) TestOptions {
	var namespaces []namespace.Metadata
	for _, nsMeta := range testOpts.Namespaces() {
		nsOpts := nsMeta.Options().SetSchemaHistory(testSchemaHistory)
		md, err := namespace.NewMetadata(nsMeta.ID(), nsOpts)
		require.NoError(t, err)
		namespaces = append(namespaces, md)
	}
	return testOpts.SetProtoEncoding(true).
		SetNamespaces(namespaces).
		SetAssertTestDataEqual(assertProtoDataEqual)
}

func assertProtoDataEqual(t *testing.T, expected, actual []generate.TestValue) bool {
	if len(expected) != len(actual) {
		return false
	}
	for i := 0; i < len(expected); i++ {
		if !assert.Equal(t, expected[i].TimestampNanos, actual[i].TimestampNanos) {
			return false
		}
		if !assert.Equal(t, expected[i].Value, actual[i].Value) {
			return false
		}
		if !prototest.ProtoEqual(testSchema, expected[i].Annotation, actual[i].Annotation) {
			return false
		}
	}
	return true
}

func testRoundtrip(t *testing.T, setTestOpts setTestOptions, updateInputConfig generate.UpdateBlockConfig) {
	// if testing.Short() {
	// 	t.SkipNow() // Just skip if we're doing a short run
	// }
	// Test setup
	testOpts := NewTestOptions(t).
		SetTickMinimumInterval(time.Second).
		SetUseTChannelClientForReading(false).
		SetUseTChannelClientForWriting(false)
	if setTestOpts != nil {
		testOpts = setTestOpts(t, testOpts)
	}
	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	// Input data setup
	blockSize := namespace.NewOptions().RetentionOptions().BlockSize()
	now := testSetup.NowFn()()
	inputData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: now},
		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: now.Add(blockSize)},
	}
	if updateInputConfig != nil {
		updateInputConfig(inputData)
	}

	// Start the server
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	log.Debug("round trip test")
	require.NoError(t, testSetup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	// Write test data
	seriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock)
	for _, input := range inputData {
		testSetup.SetNowFn(input.Start)
		testData := generate.Block(input)
		seriesMaps[input.Start] = testData
		require.NoError(t, testSetup.WriteBatch(testNamespaces[0], testData))
	}
	log.Debug("test data is now written")

	// Advance time and sleep for a long enough time so data blocks are sealed during ticking
	testSetup.SetNowFn(testSetup.NowFn()().Add(blockSize * 2))
	testSetup.SleepFor10xTickMinimumInterval()

	// Verify in-memory data match what we've written
	verifySeriesMaps(t, testSetup, testNamespaces[0], seriesMaps)

	// Verify in-memory data again just to be sure the data can be read multiple times without issues
	verifySeriesMaps(t, testSetup, testNamespaces[0], seriesMaps)
}
