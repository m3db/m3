// Copyright (c) 2018 Uber Technologies, Inc.
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

package docs

import (
	"fmt"
	"regexp"
)

var (
	// repoPathURLRegexp provides a regular expression that returns true/false
	// whether a URL is a repository path URL.
	// It uses capture groups to help us extract the relative repository path
	// that the URL points to.
	repoPathURLRegexp = regexp.MustCompile(
		`^https://github.com/m3db/.*/blob/[a-zA-Z0-9]+/([-_.a-zA-Z0-9/]+)(#?.*)(\??.*)$`)
)

const (
	// repoPathURLPathIndex is the capture group for the asset path in the URL.
	repoPathURLPathIndex = 1
)

// Path returns the url to the given section of documentation.
func Path(section string) string {
	return fmt.Sprintf("https://docs.m3db.io/%s", section)
}

// RepoPathURL is a URL that points to a path in the repository, helpful
// for identifying links that link to assets in the repository from our
// documentation.
type RepoPathURL struct {
	// RepoPath is the relative path to an asset in the repository.
	RepoPath string
}

// ParseRepoPathURL will return true and the details of the repository path
// URL if the URL is a URL that points to an asset in the repository, or
// false otherwise.
func ParseRepoPathURL(url string) (RepoPathURL, bool) {
	if !repoPathURLRegexp.MatchString(url) {
		return RepoPathURL{}, false
	}

	matches := repoPathURLRegexp.FindStringSubmatch(url)
	path := matches[repoPathURLPathIndex]
	return RepoPathURL{RepoPath: path}, true
}
