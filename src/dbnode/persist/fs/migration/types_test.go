/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package migration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestParseMigrateVersion(t *testing.T) {
	v, err := ParseMigrateVersion("none")
	require.NoError(t, err)
	require.Equal(t, MigrateVersionNone, v)

	v, err = ParseMigrateVersion("1.1")
	require.NoError(t, err)
	require.Equal(t, MigrateVersion_1_1, v)
}

func TestValidateMigrateVersion(t *testing.T) {
	err := ValidateMigrateVersion(MigrateVersion_1_1)
	require.NoError(t, err)

	err = ValidateMigrateVersion(2)
	require.Error(t, err)
}

func TestUnmarshalYAML(t *testing.T) {
	type config struct {
		Version MigrateVersion `yaml:"version"`
	}

	for _, value := range validMigrateVersions {
		str := fmt.Sprintf("version: %s\n", value.String())
		var cfg config
		require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))
		require.Equal(t, value, cfg.Version)
	}

	var cfg config
	require.Error(t, yaml.Unmarshal([]byte("version: abc"), &cfg))
}
