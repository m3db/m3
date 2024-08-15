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
	"math"
	"sort"
	"strconv"
	"strings"
)

// Response represents Prometheus's query response.
type Response struct {
	// Status is the response status.
	Status string `json:"status"`
	// Data is the response data.
	Data data `json:"data"`
}

type data struct {
	// ResultType is the type of Result (matrix, vector, etc.).
	ResultType string
	// Result contains the query result (concrete type depends on ResultType).
	Result result
}

type result interface {
	matches(other result) (MatchInformation, error)
}

// MatrixResult contains a list matrixRow.
type MatrixResult struct {
	Result []matrixRow `json:"result"`
}

// VectorResult contains a list of vectorItem.
type VectorResult struct {
	Result []vectorItem `json:"result"`
}

// ScalarResult is the scalar Value for the response.
type ScalarResult struct {
	Result Value `json:"result"`
}

// StringResult is the string Value for the response.
type StringResult struct {
	Result Value `json:"result"`
}

// UnmarshalJSON unmarshals the data struct of query response.
func (d *data) UnmarshalJSON(bytes []byte) error {
	var discriminator struct {
		ResultType string `json:"resultType"`
	}
	if err := json.Unmarshal(bytes, &discriminator); err != nil {
		return err
	}
	*d = data{ResultType: discriminator.ResultType}

	switch discriminator.ResultType {

	case "matrix":
		d.Result = &MatrixResult{}

	case "vector":
		d.Result = &VectorResult{}

	case "scalar":
		d.Result = &ScalarResult{}

	case "string":
		d.Result = &StringResult{}

	default:
		return fmt.Errorf("unknown resultType: %s", discriminator.ResultType)
	}

	return json.Unmarshal(bytes, d.Result)
}

// Len is the number of elements in the collection.
func (r MatrixResult) Len() int { return len(r.Result) }

// Less reports whether the element with
// index i should sort before the element with index j.
func (r MatrixResult) Less(i, j int) bool {
	return r.Result[i].id < r.Result[j].id
}

// Swap swaps the elements with indexes i and j.
func (r MatrixResult) Swap(i, j int) { r.Result[i], r.Result[j] = r.Result[j], r.Result[i] }

// Sort sorts the MatrixResult.
func (r MatrixResult) Sort() {
	for i, result := range r.Result {
		r.Result[i].id = result.Metric.genID()
	}

	sort.Sort(r)
}

// Len is the number of elements in the vector.
func (r VectorResult) Len() int { return len(r.Result) }

// Less reports whether the element with
// index i should sort before the element with index j.
func (r VectorResult) Less(i, j int) bool {
	return r.Result[i].id < r.Result[j].id
}

// Swap swaps the elements with indexes i and j.
func (r VectorResult) Swap(i, j int) { r.Result[i], r.Result[j] = r.Result[j], r.Result[i] }

// Sort sorts the VectorResult.
func (r VectorResult) Sort() {
	for i, result := range r.Result {
		r.Result[i].id = result.Metric.genID()
	}

	sort.Sort(r)
}

// matrixRow is a single row of "matrix" Result.
type matrixRow struct {
	// Metric is the tags for the matrixRow.
	Metric Tags `json:"metric"`
	// Values is the set of values for the matrixRow.
	Values Values `json:"values"`
	id     string
}

// vectorItem is a single item of "vector" Result.
type vectorItem struct {
	// Metric is the tags for the vectorItem.
	Metric Tags `json:"metric"`
	// Value is the value for the vectorItem.
	Value Value `json:"value"`
	id    string
}

// Tags is a simple representation of Prometheus tags.
type Tags map[string]string

// Values is a list of values for the Prometheus Result.
type Values []Value

// Value is a single value for Prometheus Result.
type Value []interface{}

func (t *Tags) genID() string {
	tags := make(sort.StringSlice, len(*t))
	for k, v := range *t {
		tags = append(tags, fmt.Sprintf("%s:%s,", k, v))
	}

	sort.Sort(tags)
	var sb strings.Builder
	// NB: this may clash but exact tag values are also checked, and this is a
	// validation endpoint so there's less concern over correctness.
	for _, t := range tags {
		sb.WriteString(t)
	}

	return sb.String()
}

// MatchInformation describes how well two responses match.
type MatchInformation struct {
	// FullMatch indicates a full match.
	FullMatch bool
	// NoMatch indicates that the responses do not match sufficiently.
	NoMatch bool
}

// Matches compares two responses and determines how closely they match.
func (r Response) Matches(other Response) (MatchInformation, error) {
	if r.Status != other.Status {
		err := fmt.Errorf("status %s does not match other status %s",
			r.Status, other.Status)
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	if r.Status == "error" {
		return MatchInformation{
			FullMatch: true,
		}, nil
	}

	return r.Data.matches(other.Data)
}

func (d data) matches(other data) (MatchInformation, error) {
	if d.ResultType != other.ResultType {
		err := fmt.Errorf("result type %s does not match other result type %s",
			d.ResultType, other.ResultType)
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	return d.Result.matches(other.Result)
}

func (r MatrixResult) matches(other result) (MatchInformation, error) {
	otherMatrix, ok := other.(*MatrixResult)
	if !ok {
		err := fmt.Errorf("incorrect type for matching, expected MatrixResult, %v", other)
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	if len(r.Result) != len(otherMatrix.Result) {
		err := fmt.Errorf("result length %d does not match other result length %d",
			len(r.Result), len(otherMatrix.Result))
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	r.Sort()
	otherMatrix.Sort()
	for i, result := range r.Result {
		if err := result.matches(otherMatrix.Result[i]); err != nil {
			return MatchInformation{
				NoMatch: true,
			}, err
		}
	}

	return MatchInformation{FullMatch: true}, nil
}

func (r VectorResult) matches(other result) (MatchInformation, error) {
	otherVector, ok := other.(*VectorResult)
	if !ok {
		err := fmt.Errorf("incorrect type for matching, expected VectorResult")
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	if len(r.Result) != len(otherVector.Result) {
		err := fmt.Errorf("result length %d does not match other result length %d",
			len(r.Result), len(otherVector.Result))
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	r.Sort()
	otherVector.Sort()
	for i, result := range r.Result {
		if err := result.matches(otherVector.Result[i]); err != nil {
			return MatchInformation{
				NoMatch: true,
			}, err
		}
	}

	return MatchInformation{FullMatch: true}, nil
}

func (r ScalarResult) matches(other result) (MatchInformation, error) {
	otherScalar, ok := other.(*ScalarResult)
	if !ok {
		err := fmt.Errorf("incorrect type for matching, expected ScalarResult")
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	if err := r.Result.matches(otherScalar.Result); err != nil {
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	return MatchInformation{FullMatch: true}, nil
}

func (r StringResult) matches(other result) (MatchInformation, error) {
	otherString, ok := other.(*StringResult)
	if !ok {
		err := fmt.Errorf("incorrect type for matching, expected StringResult")
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	if err := r.Result.matches(otherString.Result); err != nil {
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	return MatchInformation{FullMatch: true}, nil
}

func (r matrixRow) matches(other matrixRow) error {
	// NB: tags should match by here so this is more of a sanity check.
	if err := r.Metric.matches(other.Metric); err != nil {
		return err
	}

	return r.Values.matches(other.Values)
}

func (r vectorItem) matches(other vectorItem) error {
	// NB: tags should match by here so this is more of a sanity check.
	if err := r.Metric.matches(other.Metric); err != nil {
		return err
	}

	return r.Value.matches(other.Value)
}

func (t Tags) matches(other Tags) error {
	if len(t) != len(other) {
		return fmt.Errorf("tag length %d does not match other tag length %d",
			len(t), len(other))
	}

	for k, v := range t {
		if vv, ok := other[k]; ok {
			if v != vv {
				return fmt.Errorf("tag %s value %s does not match other tag value %s", k, v, vv)
			}
		} else {
			return fmt.Errorf("tag %s not found in other tagset", v)
		}
	}

	return nil
}

func (v Values) matches(other Values) error {
	if len(v) != len(other) {
		return fmt.Errorf("values length %d does not match other values length %d",
			len(v), len(other))
	}

	for i, val := range v {
		if err := val.matches(other[i]); err != nil {
			return err
		}
	}

	return nil
}

func (v Value) matches(other Value) error {
	if len(v) != 2 {
		return fmt.Errorf("value length %d must be 2", len(v))
	}

	if len(other) != 2 {
		return fmt.Errorf("other value length %d must be 2", len(other))
	}

	tsV := fmt.Sprint(v[0])
	tsOther := fmt.Sprint(other[0])
	if tsV != tsOther {
		return fmt.Errorf("ts %s does not match other ts %s", tsV, tsOther)
	}

	valV, err := strconv.ParseFloat(fmt.Sprint(v[1]), 64)
	if err != nil {
		return err
	}

	valOther, err := strconv.ParseFloat(fmt.Sprint(other[1]), 64)
	if err != nil {
		return err
	}

	if math.Abs(valV-valOther) > tolerance {
		return fmt.Errorf("point %f does not match other point %f", valV, valOther)
	}

	return nil
}
