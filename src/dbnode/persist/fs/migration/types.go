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

// Options represents the options for migrations.
type Options interface {
	// Validate validates migration options.
	Validate() error

	// SetTargetMigrationVersion sets the target version for a migration
	SetTargetMigrationVersion(value MigrationVersion) Options

	// TargetMigrationVersion is the target version for a migration.
	TargetMigrationVersion() MigrationVersion

	// SetConcurrency sets the number of concurrent workers performing migrations.
	SetConcurrency(value int) Options

	// Concurrency gets the number of concurrent workers performing migrations.
	Concurrency() int
}

// MigrationVersion is an enum that corresponds to the major and minor version number to migrate data files to.
type MigrationVersion uint
