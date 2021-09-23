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

package utils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"

	"github.com/m3db/m3/src/cmd/services/m3comparator/main/parser"

	"go.uber.org/zap"
)

// RegressionQuery is the JSON representation of a query to be compared.
type RegressionQuery struct {
	Name         string          `json:"name"`
	Query        string          `json:"query"`
	StartSeconds int64           `json:"startSeconds"`
	EndSeconds   int64           `json:"endSeconds"`
	Step         int             `json:"step"`
	Retries      int             `json:"retries"`
	Data         []parser.Series `json:"data"`
}

func parseRegressionFileToQueries(
	fileName string,
	log *zap.Logger,
) (RegressionQuery, error) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Error("could not open file", zap.Error(err))
		return RegressionQuery{}, err
	}

	defer file.Close()
	buf, err := ioutil.ReadAll(file)
	if err != nil {
		log.Error("could not read file", zap.Error(err))
		return RegressionQuery{}, err
	}

	var query RegressionQuery
	if err := json.Unmarshal(buf, &query); err != nil {
		log.Error("could not unmarshal regression query", zap.Error(err))
		return RegressionQuery{}, err
	}

	return query, err
}

// PromQLRegressionQueryGroup is a PromQLQueryGroup with a given data set.
type PromQLRegressionQueryGroup struct {
	PromQLQueryGroup
	Retries int
	Data    []parser.Series
}

func (q RegressionQuery) constructPromQL() PromQLRegressionQueryGroup {
	values := make(url.Values)
	values.Add("query", q.Query)
	values.Add("step", fmt.Sprint(q.Step))
	values.Add("start", fmt.Sprint(q.StartSeconds))
	values.Add("end", fmt.Sprint(q.EndSeconds))
	query := "/api/v1/query_range?" + values.Encode()

	retries := 1
	if q.Retries > 1 {
		retries = q.Retries
	}

	return PromQLRegressionQueryGroup{
		PromQLQueryGroup: PromQLQueryGroup{
			QueryGroup: q.Name,
			Queries:    []string{query},
		},
		Data:    q.Data,
		Retries: retries,
	}
}

func parseRegressionFileToPromQLQueryGroup(
	fileName string,
	log *zap.Logger,
) (PromQLRegressionQueryGroup, error) {
	query, err := parseRegressionFileToQueries(fileName, log)
	if err != nil {
		return PromQLRegressionQueryGroup{}, err
	}

	return query.constructPromQL(), nil
}

// ParseRegressionFilesToPromQLQueryGroup parses a directory with
// regression query files into PromQL query groups.
func ParseRegressionFilesToPromQLQueryGroup(
	directory string,
	log *zap.Logger,
) ([]PromQLRegressionQueryGroup, error) {
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		log.Info("could not read directory")
		return nil, err
	}

	if len(files) == 0 {
		log.Info("no files in directory")
		return nil, nil
	}

	groups := make([]PromQLRegressionQueryGroup, 0, len(files))
	for _, f := range files {
		filePath := filepath.Join(directory, f.Name())
		if f.IsDir() {
			log.Info("skipping file", zap.String("filePath", filePath))
		}

		group, err := parseRegressionFileToPromQLQueryGroup(filePath, log)
		if err != nil {
			log.Error("failed to parse file", zap.String("path", filePath), zap.Error(err))
			return nil, err
		}

		groups = append(groups, group)
	}

	return groups, nil
}
