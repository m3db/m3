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

package services

// Configuration configs the Options.
type Configuration struct {
	Namespaces NamespacesConfiguration `yaml:"namespaces"`
}

// NewOptions creates a new Options.
func (cfg Configuration) NewOptions() Options {
	return NewOptions().
		SetNamespaceOptions(cfg.Namespaces.NewOptions())
}

// NamespacesConfiguration configs the NamespaceOptions.
type NamespacesConfiguration struct {
	Placement string `yaml:"placement"`
	Metadata  string `yaml:"metadata"`
}

// NewOptions creates a new NamespaceOptions.
func (cfg NamespacesConfiguration) NewOptions() NamespaceOptions {
	return NewNamespaceOptions().
		SetPlacementNamespace(cfg.Placement).
		SetMetadataNamespace(cfg.Metadata)
}
