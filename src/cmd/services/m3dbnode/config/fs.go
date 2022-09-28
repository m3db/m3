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

package config

import (
	"fmt"
	"os"
)

const (
	// DefaultNewFileMode is the default new file mode.
	DefaultNewFileMode = os.FileMode(0666)
	// DefaultNewDirectoryMode is the default new directory mode.
	DefaultNewDirectoryMode = os.FileMode(0755)

	defaultFilePathPrefix                  = "/var/lib/m3db"
	defaultWriteBufferSize                 = 65536
	defaultDataReadBufferSize              = 65536
	defaultInfoReadBufferSize              = 128
	defaultSeekReadBufferSize              = 4096
	defaultThroughputLimitMbps             = 1000.0
	defaultThroughputCheckEvery            = 128
	defaultForceIndexSummariesMmapMemory   = false
	defaultForceBloomFilterMmapMemory      = false
	defaultBloomFilterFalsePositivePercent = 0.02
)

// DefaultMmapConfiguration is the default mmap configuration.
func DefaultMmapConfiguration() MmapConfiguration {
	return MmapConfiguration{
		HugeTLB: MmapHugeTLBConfiguration{
			Enabled:   true,    // Enable when on a platform that supports
			Threshold: 2 << 14, // 32kb and above mappings use huge pages
		},
	}
}

// FilesystemConfiguration is the filesystem configuration.
type FilesystemConfiguration struct {
	// File path prefix for reading/writing TSDB files
	FilePathPrefix *string `yaml:"filePathPrefix"`

	// Write buffer size
	WriteBufferSize *int `yaml:"writeBufferSize"`

	// Data read buffer size
	DataReadBufferSize *int `yaml:"dataReadBufferSize"`

	// Info metadata file read buffer size
	InfoReadBufferSize *int `yaml:"infoReadBufferSize"`

	// Seek data read buffer size
	SeekReadBufferSize *int `yaml:"seekReadBufferSize"`

	// Disk flush throughput limit in Mb/s
	ThroughputLimitMbps *float64 `yaml:"throughputLimitMbps"`

	// Disk flush throughput check interval
	ThroughputCheckEvery *int `yaml:"throughputCheckEvery"`

	// NewFileMode is the new file permissions mode to use when
	// creating files - specify as three digits, e.g. 666.
	NewFileMode *string `yaml:"newFileMode"`

	// NewDirectoryMode is the new file permissions mode to use when
	// creating directories - specify as three digits, e.g. 755.
	NewDirectoryMode *string `yaml:"newDirectoryMode"`

	// Mmap is the mmap options which features are primarily platform dependent
	Mmap *MmapConfiguration `yaml:"mmap"`

	// ForceIndexSummariesMmapMemory forces the mmap that stores the index lookup bytes
	// to be an anonymous region in memory as opposed to a file-based mmap.
	ForceIndexSummariesMmapMemory *bool `yaml:"force_index_summaries_mmap_memory"`

	// ForceBloomFilterMmapMemory forces the mmap that stores the index lookup bytes
	// to be an anonymous region in memory as opposed to a file-based mmap.
	ForceBloomFilterMmapMemory *bool `yaml:"force_bloom_filter_mmap_memory"`

	// BloomFilterFalsePositivePercent controls the target false positive percentage
	// for the bloom filters for the fileset files.
	BloomFilterFalsePositivePercent *float64 `yaml:"bloomFilterFalsePositivePercent"`
}

// Validate validates the Filesystem configuration. We use this method to validate
// fields where the validator package falls short.
func (f FilesystemConfiguration) Validate() error {
	if f.WriteBufferSize != nil && *f.WriteBufferSize < 1 {
		return fmt.Errorf(
			"fs writeBufferSize is set to: %d, but must be at least 1",
			*f.WriteBufferSize)
	}

	if f.DataReadBufferSize != nil && *f.DataReadBufferSize < 1 {
		return fmt.Errorf(
			"fs dataReadBufferSize is set to: %d, but must be at least 1",
			*f.DataReadBufferSize)
	}

	if f.InfoReadBufferSize != nil && *f.InfoReadBufferSize < 1 {
		return fmt.Errorf(
			"fs infoReadBufferSize is set to: %d, but must be at least 1",
			*f.InfoReadBufferSize)
	}

	if f.SeekReadBufferSize != nil && *f.SeekReadBufferSize < 1 {
		return fmt.Errorf(
			"fs seekReadBufferSize is set to: %d, but must be at least 1",
			*f.SeekReadBufferSize)
	}

	if f.ThroughputLimitMbps != nil && *f.ThroughputLimitMbps < 1 {
		return fmt.Errorf(
			"fs throughputLimitMbps is set to: %f, but must be at least 1",
			*f.ThroughputLimitMbps)
	}

	if f.ThroughputCheckEvery != nil && *f.ThroughputCheckEvery < 1 {
		return fmt.Errorf(
			"fs throughputCheckEvery is set to: %d, but must be at least 1",
			*f.ThroughputCheckEvery)
	}
	if f.BloomFilterFalsePositivePercent != nil &&
		(*f.BloomFilterFalsePositivePercent < 0 || *f.BloomFilterFalsePositivePercent > 1) {
		return fmt.Errorf(
			"fs bloomFilterFalsePositivePercent is set to: %f, but must be between 0.0 and 1.0",
			*f.BloomFilterFalsePositivePercent)
	}

	return nil
}

// FilePathPrefixOrDefault returns the configured file path prefix if configured, or a
// default value otherwise.
func (f FilesystemConfiguration) FilePathPrefixOrDefault() string {
	if f.FilePathPrefix != nil {
		return *f.FilePathPrefix
	}

	return defaultFilePathPrefix
}

// WriteBufferSizeOrDefault returns the configured write buffer size if configured, or a
// default value otherwise.
func (f FilesystemConfiguration) WriteBufferSizeOrDefault() int {
	if f.WriteBufferSize != nil {
		return *f.WriteBufferSize
	}

	return defaultWriteBufferSize
}

// DataReadBufferSizeOrDefault returns the configured data read buffer size if configured, or a
// default value otherwise.
func (f FilesystemConfiguration) DataReadBufferSizeOrDefault() int {
	if f.DataReadBufferSize != nil {
		return *f.DataReadBufferSize
	}

	return defaultDataReadBufferSize
}

// InfoReadBufferSizeOrDefault returns the configured info read buffer size if configured, or a
// default value otherwise.
func (f FilesystemConfiguration) InfoReadBufferSizeOrDefault() int {
	if f.InfoReadBufferSize != nil {
		return *f.InfoReadBufferSize
	}

	return defaultInfoReadBufferSize
}

// SeekReadBufferSizeOrDefault returns the configured seek read buffer size if configured, or a
// default value otherwise.
func (f FilesystemConfiguration) SeekReadBufferSizeOrDefault() int {
	if f.SeekReadBufferSize != nil {
		return *f.SeekReadBufferSize
	}

	return defaultSeekReadBufferSize
}

// ThroughputLimitMbpsOrDefault returns the configured throughput limit mbps if configured, or a
// default value otherwise.
func (f FilesystemConfiguration) ThroughputLimitMbpsOrDefault() float64 {
	if f.ThroughputLimitMbps != nil {
		return *f.ThroughputLimitMbps
	}

	return defaultThroughputLimitMbps
}

// ThroughputCheckEveryOrDefault returns the configured throughput check every value if configured, or a
// default value otherwise.
func (f FilesystemConfiguration) ThroughputCheckEveryOrDefault() int {
	if f.ThroughputCheckEvery != nil {
		return *f.ThroughputCheckEvery
	}

	return defaultThroughputCheckEvery
}

// MmapConfigurationOrDefault returns the configured mmap configuration if configured, or a
// default value otherwise.
func (f FilesystemConfiguration) MmapConfigurationOrDefault() MmapConfiguration {
	if f.Mmap == nil {
		return DefaultMmapConfiguration()
	}
	return *f.Mmap
}

// ForceIndexSummariesMmapMemoryOrDefault returns the configured value for forcing the summaries
// mmaps into anonymous region in memory if configured, or a default value otherwise.
func (f FilesystemConfiguration) ForceIndexSummariesMmapMemoryOrDefault() bool {
	if f.ForceIndexSummariesMmapMemory != nil {
		return *f.ForceIndexSummariesMmapMemory
	}

	return defaultForceIndexSummariesMmapMemory
}

// ForceBloomFilterMmapMemoryOrDefault returns the configured value for forcing the bloom
// filter mmaps into anonymous region in memory if configured, or a default value otherwise.
func (f FilesystemConfiguration) ForceBloomFilterMmapMemoryOrDefault() bool {
	if f.ForceBloomFilterMmapMemory != nil {
		return *f.ForceBloomFilterMmapMemory
	}

	return defaultForceBloomFilterMmapMemory
}

// BloomFilterFalsePositivePercentOrDefault returns the configured value for the target
// false positive percent for the bloom filter for the fileset files if configured, or a default
// value otherwise
func (f FilesystemConfiguration) BloomFilterFalsePositivePercentOrDefault() float64 {
	if f.BloomFilterFalsePositivePercent != nil {
		return *f.BloomFilterFalsePositivePercent
	}
	return defaultBloomFilterFalsePositivePercent
}

// MmapConfiguration is the mmap configuration.
type MmapConfiguration struct {
	// HugeTLB is the huge pages configuration which will only take affect
	// on platforms that support it, currently just linux
	HugeTLB MmapHugeTLBConfiguration `yaml:"hugeTLB"`
}

// MmapHugeTLBConfiguration is the mmap huge TLB configuration.
type MmapHugeTLBConfiguration struct {
	// Enabled if true or disabled if false
	Enabled bool `yaml:"enabled"`

	// Threshold is the threshold on which to use the huge TLB flag if enabled
	Threshold int64 `yaml:"threshold"`
}

// ParseNewFileMode parses the specified new file mode.
func (f FilesystemConfiguration) ParseNewFileMode() (os.FileMode, error) {
	if f.NewFileMode == nil {
		return DefaultNewFileMode, nil
	}

	str := *f.NewFileMode
	if len(str) != 3 {
		return 0, fmt.Errorf("file mode must be 3 chars long")
	}

	str = "0" + str

	var v uint32
	n, err := fmt.Sscanf(str, "%o", &v)
	if err != nil {
		return 0, fmt.Errorf("unable to parse: %v", err)
	}
	if n != 1 {
		return 0, fmt.Errorf("no value to parse")
	}
	return os.FileMode(v), nil
}

// ParseNewDirectoryMode parses the specified new directory mode.
func (f FilesystemConfiguration) ParseNewDirectoryMode() (os.FileMode, error) {
	if f.NewDirectoryMode == nil {
		return DefaultNewDirectoryMode, nil
	}

	str := *f.NewDirectoryMode
	if len(str) != 3 {
		return 0, fmt.Errorf("file mode must be 3 chars long")
	}

	str = "0" + str

	var v uint32
	n, err := fmt.Sscanf(str, "%o", &v)
	if err != nil {
		return 0, fmt.Errorf("unable to parse: %v", err)
	}
	if n != 1 {
		return 0, fmt.Errorf("no value to parse")
	}
	return os.ModeDir | os.FileMode(v), nil
}
