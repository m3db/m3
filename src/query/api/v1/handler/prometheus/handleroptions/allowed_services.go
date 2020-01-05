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

package handleroptions

type allowedServicesSet map[string]struct{}

func (a allowedServicesSet) String() []string {
	s := make([]string, 0, len(a))
	for key := range a {
		s = append(s, key)
	}
	return s
}

var (
	allowedServices = allowedServicesSet{
		M3DBServiceName:          struct{}{},
		M3AggregatorServiceName:  struct{}{},
		M3CoordinatorServiceName: struct{}{},
	}
)

// IsAllowedService returns whether a service name is a valid M3 service.
func IsAllowedService(svc string) bool {
	_, ok := allowedServices[svc]
	return ok
}

// AllowedServices returns the list of valid M3 services.
func AllowedServices() []string {
	svcs := make([]string, 0, len(allowedServices))
	for svc := range allowedServices {
		svcs = append(svcs, svc)
	}
	return svcs
}
