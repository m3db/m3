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

package main

import (
	"flag"
	"os"
	"text/template"
	"time"

	"github.com/m3db/m3/scripts/comparator/utils"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

// TemplateData is a collection of template data.
type TemplateData struct {
	Revision string
	Start    string
	End      string
	Queries  []utils.GrafanaQueries
}

func paramError(err string, log *zap.Logger) {
	log.Error(err)
	flag.Usage()
}

func main() {
	var (
		iOpts = instrument.NewOptions()
		log   = iOpts.Logger()

		pRevision  = flag.String("r", "", "the git revision")
		pQueryFile = flag.String("q", "", "the query file")
		pTemplate  = flag.String("t", "", "the template file")
		pOutput    = flag.String("o", "", "the output file")

		pStart = flag.Int64("s", time.Now().Unix(), "start")
		pEnd   = flag.Int64("e", time.Now().Unix(), "end")
	)

	flag.Parse()
	var (
		revision     = *pRevision
		qFile        = *pQueryFile
		output       = *pOutput
		templateFile = *pTemplate
	)

	if len(revision) == 0 {
		paramError("No revision found", log)
		os.Exit(1)
	}

	if len(qFile) == 0 {
		paramError("No query file found", log)
		os.Exit(1)
	}

	if len(output) == 0 {
		paramError("No output found", log)
		os.Exit(1)
	}

	if len(templateFile) == 0 {
		paramError("No template file found", log)
		os.Exit(1)
	}

	queries, err := utils.ParseFileToGrafanaQueries(qFile, log)
	if err != nil {
		log.Error("could not parse file to Grafana queries", zap.Error(err))
		os.Exit(1)
	}

	opts := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	outputFile, err := os.OpenFile(output, opts, 0777)
	if err != nil {
		log.Error("could not open output file", zap.Error(err))
		os.Exit(1)
	}

	defer outputFile.Close()

	start := time.Unix(*pStart, 0)
	end := time.Unix(*pEnd, 0)

	templateData := TemplateData{
		Revision: revision,
		Queries:  queries,
		Start:    start.Format(time.RFC3339),
		End:      end.Format(time.RFC3339),
	}

	t := template.Must(template.ParseFiles(templateFile))
	err = t.Execute(outputFile, templateData)
	if err != nil {
		log.Error("could not write to output file", zap.Error(err))
		os.Exit(1)
	}
}
