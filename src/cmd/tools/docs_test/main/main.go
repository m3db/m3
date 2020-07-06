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
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/m3db/m3/src/x/docs"

	"github.com/russross/blackfriday"
)

func main() {
	var (
		docsDirArg   = flag.String("docsdir", "docs", "The documents directory to test")
		linkSleepArg = flag.Duration("linksleep", time.Second, "Amount to sleep between checking links to avoid 429 rate limits from github")
	)
	flag.Parse()

	if *docsDirArg == "" {
		flag.Usage()
		os.Exit(1)
	}

	docsDir := *docsDirArg
	linkSleep := *linkSleepArg

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

				resolvedPath := ""
				if parse, ok := docs.ParseRepoPathURL(url); ok {
					// Internal relative local repo path
					resolvedPath = parse.RepoPath
				} else if isHTTPOrHTTPS(url) {
					// External link
					resolvedPath = url
				} else {
					// Otherwise should be a direct relative link
					// (not repo URL prefixed)
					if v := strings.Index(url, "#"); v != -1 {
						// Remove links to subsections of docs
						url = url[:v]
					}
					if len(url) > 0 && url[0] == '/' {
						// This is an absolute link to within the docs directory
						resolvedPath = path.Join(docsDir, url[1:])
					} else {
						// We assume this is a relative path from the current doc
						resolvedPath = path.Join(path.Dir(filePath), url)
					}
				}

				checked := checkedLink{
					document:     filePath,
					url:          url,
					resolvedPath: resolvedPath,
				}
				if isHTTPOrHTTPS(resolvedPath) {
					// Check external link using HEAD request
					client := &http.Client{
						Transport: &http.Transport{
							// Some sites have invalid certs, like some kubernetes docs
							TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
						},
					}
					resp, err := client.Head(resolvedPath)
					if err == nil {
						class := resp.StatusCode / 100
						// Ignore 5xx errors since they are indicative of a problem with the external server
						// and not our documentation.
						if class != 2 && class != 5 {
							err = fmt.Errorf("unexpected status code: status=%v", resp.StatusCode)
						}
					}
					if err != nil {
						return abort(checkedLinkError(checked, err))
					}
					// Sleep to avoid rate limits.
					time.Sleep(linkSleep)
				} else {
					// Check relative path
					if _, err := os.Stat(resolvedPath); err != nil {
						return abort(checkedLinkError(checked, err))
					}
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

func isHTTPOrHTTPS(url string) bool {
	return strings.HasPrefix(strings.ToLower(url), "http://") ||
		strings.HasPrefix(strings.ToLower(url), "https://")
}

type checkedLink struct {
	document     string
	url          string
	resolvedPath string
}

func checkedLinkError(l checkedLink, err error) error {
	return fmt.Errorf(
		"could not validate URL link: document=%s, link=%s, resolved_path=%s, err=%v",
		l.document, l.url, l.resolvedPath, err)
}
