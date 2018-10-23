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
//

package config

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	xconfig "github.com/m3db/m3x/config"

	"github.com/stretchr/testify/require"
)

// TestProvidedConfigFiles ensures that the files in this directly are all valid, and will load.
func TestProvidedConfigFiles(t *testing.T) {
	cfgFiles, err := filepath.Glob("./*.yml")
	require.NoError(t, err)
	require.True(t, len(cfgFiles) > 0,
		"expected some config files in this directory. Move or remove this test if this is no longer true.")

	for _, fname := range cfgFiles {
		t.Run(fmt.Sprintf("load %s", filepath.Base(fname)), func(t *testing.T) {
			var cfg config.Configuration
			require.NoError(t, xconfig.LoadFile(&cfg, fname, xconfig.Options{
				DisableValidate: false,
			}))
		})
	}
}
