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

package deploy

type mockInstance struct {
	id          string
	revision    string
	isHealthy   bool
	isDeploying bool
}

func (m *mockInstance) ID() string        { return m.id }
func (m *mockInstance) Revision() string  { return m.revision }
func (m *mockInstance) IsHealthy() bool   { return m.isHealthy }
func (m *mockInstance) IsDeploying() bool { return m.isDeploying }

type queryAllFn func() ([]Instance, error)
type queryFn func(instanceIDs []string) ([]Instance, error)
type deployFn func(instanceIDs []string, revision string) error

type mockManager struct {
	queryAllFn queryAllFn
	queryFn    queryFn
	deployFn   deployFn
}

func (m *mockManager) QueryAll() ([]Instance, error) { return m.queryAllFn() }

func (m *mockManager) Query(instanceIDs []string) ([]Instance, error) {
	return m.queryFn(instanceIDs)
}

func (m *mockManager) Deploy(instanceIDs []string, revision string) error {
	return m.deployFn(instanceIDs, revision)
}
