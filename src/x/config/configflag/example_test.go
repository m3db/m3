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

package configflag_test

import (
	"flag"
	"fmt"

	"github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/config/configflag"
)

// The FlagStringSlice allows for multiple values when used as a flag variable.
func ExampleFlagStringSlice() {
	var configFiles configflag.FlagStringSlice
	fs := flag.NewFlagSet("config", flag.PanicOnError)
	fs.Var(&configFiles, "f", "config files")
	noError(fs.Parse([]string{"-f", "file1.yaml", "-f", "file2.yaml", "-f", "file3.yaml"}))

	fmt.Println("Config files:", configFiles.Value)
	// Output:
	// Config files: [file1.yaml file2.yaml file3.yaml]
}

// Options supports registration of config related flags, followed by config
// loading.
func ExampleOptionsRegister() {
	var cfgOpts configflag.Options

	var flags flag.FlagSet

	// normal use would use Register() (default flagset)
	cfgOpts.RegisterFlagSet(&flags)

	noError(flags.Parse([]string{"-f", "./testdata/config1.yaml", "-f", "./testdata/config2.yaml"}))

	var cfg struct {
		Foo int    `yaml:"foo"`
		Bar string `yaml:"bar"`
	}
	noError(cfgOpts.MainLoad(&cfg, config.Options{}))

	fmt.Println(cfg)
	// Output: {1 bar}
}

func noError(err error) {
	if err != nil {
		panic(err.Error())
	}
}
