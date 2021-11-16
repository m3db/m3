// Copyright (c) 2021  Uber Technologies, Inc.
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

package route

const (
	// NameReplace is the parameter that gets replaced.
	NameReplace = "name"

	// LabelValuesURL returns the url for the label values endpoint.
	LabelValuesURL = Prefix + "/label/{" + NameReplace + "}/values"

	// LabelNamesURL returns the url for the label names endpoint.
	LabelNamesURL = Prefix + "/labels"

	// QueryRangeURL return the url for the query range endpoint.
	QueryRangeURL = Prefix + "/query_range"

	// QueryURL return the url for the query endpoint.
	QueryURL = Prefix + "/query"

	// SeriesMatchURL is the url for remote prom series matcher handler.
	SeriesMatchURL = Prefix + "/series"
)
