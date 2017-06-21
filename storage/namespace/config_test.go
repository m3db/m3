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

package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/ts"
)

func TestRegistryConfig(t *testing.T) {
	var (
		needsBootstrap = false
		config         = &RegistryConfiguration{
			Metadatas: []MetadataConfiguration{
				MetadataConfiguration{
					ID: "abc",
				},
				MetadataConfiguration{
					ID:             "cde",
					NeedsBootstrap: &needsBootstrap,
				},
			},
		}
	)

	reg := config.Registry()
	md, err := reg.Get(ts.StringID("abc"))
	require.NoError(t, err)
	require.Equal(t, config.Metadatas[0].Metadata().ID().String(), md.ID().String())
	require.Equal(t, config.Metadatas[0].Metadata().Options(), md.Options())

	md, err = reg.Get(ts.StringID("cde"))
	require.NoError(t, err)
	require.Equal(t, config.Metadatas[1].Metadata().ID().String(), md.ID().String())
	require.Equal(t, config.Metadatas[1].Metadata().Options(), md.Options())

	_, err = reg.Get(ts.StringID("otherstring"))
	require.Error(t, err)
}

func TestMetadataConfig(t *testing.T) {
	var (
		id                  = "someLongString"
		needsBootstrap      = true
		needsFlush          = false
		writesToCommitLog   = true
		needsFilesetCleanup = false
		needsRepair         = false
		retention           = retention.Configuration{}
		config              = &MetadataConfiguration{
			ID:                  id,
			NeedsBootstrap:      &needsBootstrap,
			NeedsFlush:          &needsFlush,
			WritesToCommitLog:   &writesToCommitLog,
			NeedsFilesetCleanup: &needsFilesetCleanup,
			NeedsRepair:         &needsRepair,
			Retention:           &retention,
		}
	)

	metadata := config.Metadata()
	require.Equal(t, id, metadata.ID().String())

	opts := metadata.Options()
	require.Equal(t, needsBootstrap, opts.NeedsBootstrap())
	require.Equal(t, needsFlush, opts.NeedsFlush())
	require.Equal(t, writesToCommitLog, opts.WritesToCommitLog())
	require.Equal(t, needsFilesetCleanup, opts.NeedsFilesetCleanup())
	require.Equal(t, needsRepair, opts.NeedsRepair())
	require.Equal(t, retention.Options(), opts.RetentionOptions())
}
