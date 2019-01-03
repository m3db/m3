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

package debug

import (
	"encoding/json"
	"io"
	"os"
)

// hostDataSource is Source implementation returning data about the underlying host:
// PID, working directory, etc.
type hostInfoSource struct{}

type hostInfo struct {
	PID int    `json:"pid"`
	CWD string `json:"cwd"`
}

// NewHostInfoSource returns a Source for host information
func NewHostInfoSource() Source {
	return &hostInfoSource{}
}

// Write fetches data about the host and writes it in the given writer.
// The data is formatted in json.
// It will return an error if it can't get working directory or marshal.
func (h *hostInfoSource) Write(w io.Writer) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	hostInfo := &hostInfo{
		PID: os.Getpid(),
		CWD: wd,
	}
	jsonData, err := json.MarshalIndent(hostInfo, "", "  ")
	if err != nil {
		return err
	}
	w.Write(jsonData)
	return nil
}
