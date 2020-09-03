// Copyright (c) 2020 Uber Technologies, Inc.
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

package prometheus

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	fullMatch = MatchInformation{FullMatch: true}
	noMatch   = MatchInformation{NoMatch: true}
)

func TestUnmarshalPrometheusResponse(t *testing.T) {
	tests := []struct {
		name         string
		givenJson    string
		wantResponse Response
	}{
		{
			name: "status: error",
			givenJson: `{
				"status": "error",
				"errorType": "bad_data",
				"error": "invalid parameter"
			}`,
			wantResponse: Response{
				Status: "error",
			},
		},
		{
			name: "resultType: scalar",
			givenJson: `{
				"status": "success",
				"data": {
					"resultType": "scalar",
					"result": [1590605774, "84"]
				}
			}`,
			wantResponse: Response{
				"success",
				data{
					"scalar",
					&ScalarResult{Value{1590605774.0, "84"}},
				},
			},
		},
		{
			name: "resultType: string",
			givenJson: `{
				"status": "success",
				"data": {
					"resultType": "string",
					"result": [1590605775, "FOO"]
				}
			}`,
			wantResponse: Response{
				"success",
				data{
					"string",
					&StringResult{Value{1590605775.0, "FOO"}},
				},
			},
		},
		{
			name: "resultType: vector",
			givenJson: `{
				"status": "success",
				"data": {
					"resultType": "vector",
					"result": [
						{
							"metric": {
								"__name__": "foo",
								"bar": "1"
							},
							"value": [1590605775, "0.5"]
						},
						{
							"metric": {
								"__name__": "foo",
								"bar": "2"
							},
							"value": [1590605776, "2"]
						}
					]
				}
			}`,
			wantResponse: Response{
				"success",
				data{
					"vector",
					&VectorResult{[]vectorItem{
						{
							Metric: Tags{"__name__": "foo", "bar": "1"},
							Value:  Value{1590605775.0, "0.5"},
						},
						{
							Metric: Tags{"__name__": "foo", "bar": "2"},
							Value:  Value{1590605776.0, "2"},
						},
					}},
				},
			},
		},
		{
			name: "resultType: matrix",
			givenJson: `{
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": {
								"__name__": "foo",
								"bar": "1"
							},
							"values": [[1590605775, "1"], [1590605785, "11"]]
						},
						{
							"metric": {
								"__name__": "foo",
								"bar": "2"
							},
							"values": [[1590605776, "2"], [1590605786, "22"]]
						}
					]
				}
			}`,
			wantResponse: Response{
				"success",
				data{
					"matrix",
					&MatrixResult{[]matrixRow{
						{
							Metric: Tags{"__name__": "foo", "bar": "1"},
							Values: Values{{1590605775.0, "1"}, {1590605785.0, "11"}},
						},
						{
							Metric: Tags{"__name__": "foo", "bar": "2"},
							Values: Values{{1590605776.0, "2"}, {1590605786.0, "22"}},
						},
					}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &Response{}
			err := json.Unmarshal([]byte(tt.givenJson), response)
			require.NoError(t, err)
			assert.Equal(t, tt.wantResponse, *response)
		})
	}
}

func TestResponseMatching(t *testing.T) {
	tests := []struct {
		name     string
		response Response
	}{
		{
			name: "error",
			response: Response{
				Status: "error",
			},
		},

		{
			name: "scalar",
			response: Response{
				"success",
				data{
					"scalar",
					&ScalarResult{Value{1590605774.0, "1"}},
				},
			},
		},
		{
			name: "scalar other timestamp",
			response: Response{
				"success",
				data{
					"scalar",
					&ScalarResult{Value{1590605775.0, "1"}},
				},
			},
		},
		{
			name: "scalar other value",
			response: Response{
				"success",
				data{
					"scalar",
					&ScalarResult{Value{1590605774.0, "2"}},
				},
			},
		},

		{
			name: "vector",
			response: Response{
				"success",
				data{
					"vector",
					&VectorResult{[]vectorItem{
						{
							Metric: Tags{"__name__": "foo"},
							Value:  Value{1590605775.0, "0.5"},
						},
					}},
				},
			},
		},
		{
			name: "vector more tags",
			response: Response{
				"success",
				data{
					"vector",
					&VectorResult{[]vectorItem{
						{
							Metric: Tags{"__name__": "foo", "bar": "1"},
							Value:  Value{1590605775.0, "0.5"},
						},
					}},
				},
			},
		},
		{
			name: "vector more items",
			response: Response{
				"success",
				data{
					"vector",
					&VectorResult{[]vectorItem{
						{
							Metric: Tags{"__name__": "foo", "bar": "1"},
							Value:  Value{1590605775.0, "0.5"},
						},
						{
							Metric: Tags{"__name__": "foo", "bar": "2"},
							Value:  Value{1590605775.0, "0.5"},
						},
					}},
				},
			},
		},
		{
			name: "vector different tag",
			response: Response{
				"success",
				data{
					"vector",
					&VectorResult{[]vectorItem{
						{
							Metric: Tags{"__name__": "bar"},
							Value:  Value{1590605775.0, "0.5"},
						},
					}},
				},
			},
		},
		{
			name: "vector different value",
			response: Response{
				"success",
				data{
					"vector",
					&VectorResult{[]vectorItem{
						{
							Metric: Tags{"__name__": "foo"},
							Value:  Value{1590605775.0, "1"},
						},
					}},
				},
			},
		},
		{
			name: "vector different timestamp",
			response: Response{
				"success",
				data{
					"vector",
					&VectorResult{[]vectorItem{
						{
							Metric: Tags{"__name__": "foo"},
							Value:  Value{1590605774.0, "0.5"},
						},
					}},
				},
			},
		},

		{
			name: "matrix",
			response: Response{
				"success",
				data{
					"matrix",
					&MatrixResult{[]matrixRow{
						{
							Metric: Tags{"__name__": "foo"},
							Values: Values{{1590605775.0, "1"}},
						},
					}},
				},
			},
		},
		{
			name: "matrix other tag",
			response: Response{
				"success",
				data{
					"matrix",
					&MatrixResult{[]matrixRow{
						{
							Metric: Tags{"__name__": "bar"},
							Values: Values{{1590605775.0, "1"}},
						},
					}},
				},
			},
		},
		{
			name: "matrix other value",
			response: Response{
				"success",
				data{
					"matrix",
					&MatrixResult{[]matrixRow{
						{
							Metric: Tags{"__name__": "foo"},
							Values: Values{{1590605775.0, "2"}},
						},
					}},
				},
			},
		},
		{
			name: "matrix other timestamp",
			response: Response{
				"success",
				data{
					"matrix",
					&MatrixResult{[]matrixRow{
						{
							Metric: Tags{"__name__": "foo"},
							Values: Values{{1590605776.0, "1"}},
						},
					}},
				},
			},
		},
		{
			name: "matrix more tags",
			response: Response{
				"success",
				data{
					"matrix",
					&MatrixResult{[]matrixRow{
						{
							Metric: Tags{"__name__": "foo", "bar": "1"},
							Values: Values{{1590605775.0, "1"}},
						},
					}},
				},
			},
		},
		{
			name: "matrix more values",
			response: Response{
				"success",
				data{
					"matrix",
					&MatrixResult{[]matrixRow{
						{
							Metric: Tags{"__name__": "foo"},
							Values: Values{{1590605775.0, "1"}, {1590605776.0, "2"}},
						},
					}},
				},
			},
		},
		{
			name: "matrix more rows",
			response: Response{
				"success",
				data{
					"matrix",
					&MatrixResult{[]matrixRow{
						{
							Metric: Tags{"__name__": "foo"},
							Values: Values{{1590605775.0, "1"}},
						},
						{
							Metric: Tags{"__name__": "bar"},
							Values: Values{{1590605775.0, "1"}},
						},
					}},
				},
			},
		},
	}

	for i, ti := range tests {
		for j, tj := range tests {
			t.Run(fmt.Sprintf("%s vs %s", ti.name, tj.name), func(t *testing.T) {
				matchResult, err := ti.response.Matches(tj.response)
				if i == j { // should match
					require.NoError(t, err)
					assert.Equal(t, fullMatch, matchResult)
				} else { // should not match
					require.Error(t, err)
					assert.Equal(t, noMatch, matchResult)
				}
			})
		}
	}
}

func TestResponseMatchingOrderInsensitive(t *testing.T) {
	tests := []struct {
		name  string
		left  Response
		right Response
	}{
		{
			name: "vector",
			left: Response{
				"success",
				data{
					"vector",
					&VectorResult{[]vectorItem{
						{
							Metric: Tags{"__name__": "first"},
							Value:  Value{1590605775.0, "1"},
						},
						{
							Metric: Tags{"__name__": "second"},
							Value:  Value{1590605775.0, "2"},
						},
					}},
				},
			},
			right: Response{
				"success",
				data{
					"vector",
					&VectorResult{[]vectorItem{
						{
							Metric: Tags{"__name__": "second"},
							Value:  Value{1590605775.0, "2"},
						},
						{
							Metric: Tags{"__name__": "first"},
							Value:  Value{1590605775.0, "1"},
						},
					}},
				},
			},
		},
		{
			name: "matrix",
			left: Response{
				"success",
				data{
					"matrix",
					&MatrixResult{[]matrixRow{
						{
							Metric: Tags{"__name__": "first"},
							Values: Values{{1590605775.0, "1"}},
						},
						{
							Metric: Tags{"__name__": "second"},
							Values: Values{{1590605775.0, "2"}},
						},
					}},
				},
			},
			right: Response{
				"success",
				data{
					"matrix",
					&MatrixResult{[]matrixRow{
						{
							Metric: Tags{"__name__": "second"},
							Values: Values{{1590605775.0, "2"}},
						},
						{
							Metric: Tags{"__name__": "first"},
							Values: Values{{1590605775.0, "1"}},
						},
					}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf(tt.name), func(t *testing.T) {
			matchResult, err := tt.left.Matches(tt.right)
			require.NoError(t, err)
			assert.Equal(t, fullMatch, matchResult)
		})
	}
}
