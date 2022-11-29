// Copyright (c) 2019 Uber Technologies, Inc.
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

package configflag

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/m3db/m3/src/x/config"
)

var _ flag.Value = (*FlagStringSlice)(nil)

// Options represents the values of config command line flags
type Options struct {
	// set by commandline flags
	// ConfigFiles (-f) is a list of config files to load
	ConfigFiles FlagStringSlice

	// ShouldDumpConfigAndExit (-d) causes MainLoad to print config to stdout,
	// and then exit.
	ShouldDumpConfigAndExit bool

	// ShouldValidateConfigAndExit (-z) causes MainLoad to validate the config
	// and return an error if the config is invalid.
	ShouldValidateConfigAndExit bool

	// for Usage()
	cmd *flag.FlagSet

	// test mocking options
	osFns osIface
}

// Register registers commandline options with the default flagset.
func (opts *Options) Register() {
	opts.RegisterFlagSet(flag.CommandLine)
}

// RegisterFlagSet registers commandline options with the given flagset.
func (opts *Options) RegisterFlagSet(cmd *flag.FlagSet) {
	opts.cmd = cmd

	cmd.Var(&opts.ConfigFiles, "f", "Configuration files to load")
	cmd.BoolVar(&opts.ShouldDumpConfigAndExit, "d", false, "Dump configuration and exit")
	cmd.BoolVar(&opts.ShouldValidateConfigAndExit, "z", false, "Validate configuration and exit")
}

// MainLoad is a convenience method, intended for use in main(), which handles all
// config commandline options. It:
//  - Dumps config and exits if -d was passed.
//  - Loads configuration otherwise.
// Users who want a subset of this behavior should call individual methods.
func (opts *Options) MainLoad(target interface{}, loadOpts config.Options) error {
	osFns := opts.osFns
	if osFns == nil {
		osFns = realOS{}
	}

	if len(opts.ConfigFiles.Value) == 0 {
		opts.cmd.Usage()
		return errors.New("-f is required (no config files provided)")
	}

	if err := config.LoadFiles(target, opts.ConfigFiles.Value, loadOpts); err != nil {
		return fmt.Errorf("unable to load config from %s: %v", opts.ConfigFiles.Value, err)
	}

	if opts.ShouldDumpConfigAndExit {
		if err := config.Dump(target, osFns.Stdout()); err != nil {
			return fmt.Errorf("failed to dump config: %v", err)
		}

		osFns.Exit(0)
	}
	return nil
}

// FlagStringSlice represents a slice of strings. When used as a flag variable,
// it allows for multiple string values. For example, it can be used like this:
// 	var configFiles FlagStringSlice
// 	flag.Var(&configFiles, "f", "configuration file(s)")
// Then it can be invoked like this:
// 	./app -f file1.yaml -f file2.yaml -f valueN.yaml
// Finally, when the flags are parsed, the variable contains all the values.
type FlagStringSlice struct {
	Value []string

	overridden bool
}

// String() returns a string implementation of the slice.
func (i *FlagStringSlice) String() string {
	if i == nil {
		return ""
	}
	return fmt.Sprintf("%v", i.Value)
}

// Set appends a string value to the slice.
func (i *FlagStringSlice) Set(value string) error {
	// on first call, reset
	// afterwards, append. This allows better defaulting behavior (defaults
	// are overridden by explicitly specified flags).
	if !i.overridden {
		// make this a new slice.
		i.overridden = true
		i.Value = nil
	}

	i.Value = append(i.Value, value)
	return nil
}

type osIface interface {
	Exit(status int)
	Stdout() io.Writer
}

type realOS struct{}

func (r realOS) Exit(status int) {
	os.Exit(status)
}

func (r realOS) Stdout() io.Writer {
	return os.Stdout
}
