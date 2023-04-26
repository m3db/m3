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

const (
	// MigrationVersionNone indicates node should not attempt to perform any migrations.
	MigrationVersionNone MigrationVersion = iota
	// MigrationVersion_1_1 indicates node should attempt to migrate data files up to version 1.1.
	MigrationVersion_1_1
)

var (
	validMigrationVersions = []MigrationVersion{
		MigrationVersionNone,
		MigrationVersion_1_1,
	}
)

func (m *MigrationVersion) String() string {
	switch *m {
	case MigrationVersionNone:
		return "none"
	case MigrationVersion_1_1:
		return "1.1"
	default:
		return "unknown"
	}
}

// ParseMigrationVersion parses a string for a MigrationVersion.
func ParseMigrationVersion(str string) (MigrationVersion, error) {
	for _, valid := range validMigrationVersions {
		if str == valid.String() {
			return valid, nil
		}
	}

	return 0, fmt.Errorf("unrecognized migrate version: %v", str)
}

// ValidateMigrationVersion validates a stored metrics type.
func ValidateMigrationVersion(m MigrationVersion) error {
	for _, valid := range validMigrationVersions {
		if valid == m {
			return nil
		}
	}

	return fmt.Errorf("invalid migrate version '%v': should be one of %v",
		m, validMigrationVersions)
}

// MarshalYAML marshals a MigrationVersion.
func (m MigrationVersion) MarshalYAML() (interface{}, error) {
	return m.String(), nil
}

// UnmarshalYAML unmarshals a migrate version.
func (m *MigrationVersion) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	if value, err := ParseMigrationVersion(str); err == nil {
		*m = value
		return nil
	}

	return fmt.Errorf("invalid MigrationVersion '%s' valid types are: %v",
		str, validMigrationVersions)
}
