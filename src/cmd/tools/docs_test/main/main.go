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

	"github.com/pborman/getopt"
	"gopkg.in/russross/blackfriday.v2"
)

func main() {
	// Default link excludes.
	linkExcludes := []string{
		// Youtube has some problematic public rate limits.
		"youtu.be",
		"youtube.com",
	}

	var (
		docsDirArg   = getopt.StringLong("docsdir", 'd', "docs", "The documents directory to test")
		linkSleepArg = getopt.DurationLong("linksleep", 's', time.Second, "Amount to sleep between checking links to avoid 429 rate limits from github")
	)
	getopt.ListVarLong(&linkExcludes, "linkexcludes", 'e', "Exclude strings to check links against due to rate limiting/flakiness")
	getopt.Parse()
	if len(*docsDirArg) == 0 {
		getopt.Usage()
		return
	}

	docsDir := *docsDirArg
	linkSleep := *linkSleepArg

	var (
		excluded           []string
		repoPathsValidated int
	)
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
				linkURL := strings.TrimSpace(string(link.Destination))

				resolvedPath := ""
				if parse, ok := docs.ParseRepoPathURL(linkURL); ok {
					// Internal relative local repo path
					resolvedPath = parse.RepoPath
				} else if isHTTPOrHTTPS(linkURL) {
					// External link
					resolvedPath = linkURL
				} else {
					// Otherwise should be a direct relative link
					// (not repo URL prefixed)
					if v := strings.Index(linkURL, "#"); v != -1 {
						// Remove links to subsections of docs
						linkURL = linkURL[:v]
					}
					if len(linkURL) > 0 && linkURL[0] == '/' {
						// This is an absolute link to within the docs directory
						resolvedPath = path.Join(docsDir, linkURL[1:])
					} else {
						// We assume this is a relative path from the current doc
						resolvedPath = path.Join(path.Dir(filePath), linkURL)
					}
				}

				checked := checkedLink{
					document:     filePath,
					url:          linkURL,
					resolvedPath: resolvedPath,
				}
				if isHTTPOrHTTPS(resolvedPath) {
					excludeCheck := false
					for _, str := range linkExcludes {
						if strings.Contains(resolvedPath, str) {
							excludeCheck = true
							break
						}
					}
					if excludeCheck {
						// Exclude this link and walk to next.
						excluded = append(excluded, resolvedPath)
						return blackfriday.GoToNext
					}

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

	log.Printf("validated docs (repoPathsValidated=%d, excluded=%d)\n",
		repoPathsValidated, len(excluded))

	for _, linkURL := range excluded {
		log.Printf("excluded: %s\n", linkURL)
	}
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
