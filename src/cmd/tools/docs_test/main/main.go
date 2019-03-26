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

// docs_test is a tool to run sanity tests on the documentation before publishing.
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/m3db/m3/src/x/docs"

	"github.com/russross/blackfriday"
)

func main() {
	var (
		docsDirArg = flag.String("docsdir", "docs", "The documents directory to test")
	)
	flag.Parse()

	if *docsDirArg == "" {
		flag.Usage()
		os.Exit(1)
	}

	docsDir := *docsDirArg

	repoPathsValidated := 0
	resultErr := filepath.Walk(docsDir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("failed to walk path=%s: %v", filePath, err)
		}
		if !strings.HasSuffix(filePath, ".md") {
			return nil
		}

		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read path=%s: %v", filePath, err)
		}

		var validateErr error
		abort := func(err error) blackfriday.WalkStatus {
			validateErr = err
			return blackfriday.Terminate
		}

		md := blackfriday.New()
		md.Parse(data).Walk(func(node *blackfriday.Node, entering bool) blackfriday.WalkStatus {
			switch node.Type {
			case blackfriday.Link:
				link := node.LinkData
				url := strings.TrimSpace(string(link.Destination))

				parse, ok := docs.ParseRepoPathURL(url)
				if !ok {
					break
				}

				if _, err := os.Stat(parse.RepoPath); err != nil {
					return abort(fmt.Errorf(
						"could not stat repo path: link=%s, path=%s, err=%v",
						url, parse.RepoPath, err))
				}

				// Valid
				repoPathsValidated++
			}
			return blackfriday.GoToNext
		})

		return validateErr
	})
	if resultErr != nil {
		log.Fatalf("failed validation: %v", resultErr)
	}

	log.Printf("validated docs (repoPathsValidated=%d)\n", repoPathsValidated)
}
