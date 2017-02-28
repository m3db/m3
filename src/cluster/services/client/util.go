// Copyright (c) 2016 Uber Technologies, Inc.
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

package client

import (
	"fmt"

	"github.com/m3db/m3cluster/services"
)

const (
	placementPrefix = "_sd.placement"
	metadataPrefix  = "_sd.metadata"
	keyFormat       = "%s/%s"
)

func adKey(sid services.ServiceID, id string) string {
	return fmt.Sprintf(keyFormat, serviceKey(sid), id)
}

func placementKey(s services.ServiceID) string {
	return fmt.Sprintf(keyFormat, placementPrefix, serviceKey(s))
}

func metadataKey(s services.ServiceID) string {
	return fmt.Sprintf(keyFormat, metadataPrefix, serviceKey(s))
}

func serviceKey(s services.ServiceID) string {
	if s.Environment() == "" {
		return s.Name()
	}
	return fmt.Sprintf(keyFormat, s.Environment(), s.Name())
}
