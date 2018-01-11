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

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	xlog "github.com/m3db/m3x/log"
)

var (
	pkg  = flag.String("pkg", "", "package mock is being generated for")
	out  = flag.String("out", "", "output path for mock being written")
	perm = flag.String("perm", "666", "permissions to write file with")
)

func main() {
	logger, err := xlog.Configuration{}.BuildLogger()
	if err != nil {
		log.Fatalf("unable to build logger: %v", err)
	}

	flag.Parse()

	newFileMode, err := parseNewFileMode(*perm)
	if err != nil {
		logger.Errorf("perm: %v", err)
	}

	if len(*pkg) == 0 || len(*out) == 0 || err != nil {
		flag.Usage()
		os.Exit(1)
	}

	pkgParts := strings.Split(*pkg, "/")
	basePkg := pkgParts[len(pkgParts)-1]

	replacer := strings.NewReplacer(
		// Replace any self referential imports
		fmt.Sprintf("%s \"%s\"", basePkg, *pkg), "",
		fmt.Sprintf("%s.", basePkg), "")

	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		logger.Fatalf("unable to read input: %v", err)
	}

	replaced := []byte(replacer.Replace(string(data)))

	err = ioutil.WriteFile(*out, replaced, newFileMode)
	if err != nil {
		logger.Fatalf("unable to write output to %s: %v", *out, err)
	}
}

func parseNewFileMode(str string) (os.FileMode, error) {
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
