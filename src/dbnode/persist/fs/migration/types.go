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

package migration

import "fmt"

// Options represents the options for migrations
type Options interface {
	// Validate validates migration options
	Validate() error

	// SetToVersion sets the toVersion migration option
	SetToVersion(value MigrateVersion) Options

	// ToVersion returns the value of toVersion migration option
	ToVersion() MigrateVersion

	// SetConcurrency sets the number of concurrent workers performing migrations
	SetConcurrency(value int) Options

	// Concurrency gets the number of concurrent workers performing migrations
	Concurrency() int
}

// MigrateVersion is an enum that corresponds to the major and minor version number to migrate data files to
type MigrateVersion uint

const (
	// MigrateVersionNone indicates node should not attempt to perform any migrations
	MigrateVersionNone MigrateVersion = iota
	// MigrateVersion_1_1 indicates node should attempt to migrate data files up to version 1.1
	MigrateVersion_1_1
)

var (
	validMigrateVersions = []MigrateVersion{
		MigrateVersionNone,
		MigrateVersion_1_1,
	}
)

func (m *MigrateVersion) String() string {
	switch *m {
	case MigrateVersionNone:
		return "none"
	case MigrateVersion_1_1:
		return "1.1"
	default:
		return "unknown"
	}
}

// ParseMigrateVersion parses a string for a MigrateVersion
func ParseMigrateVersion(str string) (MigrateVersion, error) {
	for _, valid := range validMigrateVersions {
		if str == valid.String() {
			return valid, nil
		}
	}

	return 0, fmt.Errorf("unrecognized migrate version: %v", str)
}

// ValidateMigrateVersion validates a stored metrics type.
func ValidateMigrateVersion(m MigrateVersion) error {
	for _, valid := range validMigrateVersions {
		if valid == m {
			return nil
		}
	}

	return fmt.Errorf("invalid migrate version '%v': should be one of %v",
		m, validMigrateVersions)
}

// UnmarshalYAML unmarshals a migrate version
func (m *MigrateVersion) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	if value, err := ParseMigrateVersion(str); err == nil {
		*m = value
		return nil
	}

	return fmt.Errorf("invalid MigrateVersion '%s' valid types are: %v",
		str, validMigrateVersions)
}
