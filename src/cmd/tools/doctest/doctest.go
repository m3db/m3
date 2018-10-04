// Copyright (c) 2018 Uber Technologies, Inc.
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
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
)

const (
	operationRegEx  = `(?s)<operation>.*?</operation>|(?s)<operation_json>.*?</operation_json>`
	validationRegEx = `(?s)<validation>.*?</validation>`

	operationOpen      = `<operation>`
	operationClose     = `</operation>`
	operationJSONOpen  = `<operation_json>`
	operationJSONClose = `</operation_json>`
	validationOpen     = `<validation>`
	validationClose    = `</validation>`

	all = `<operation>|</operation>|<validation>|</validation>`
)

var (
	file string
)

func init() {
	flag.StringVar(&file, "file", "", "File to be parsed")
}

func main() {
	flag.Parse()

	f, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("unable to read source file: %v\n", err)
	}

	validationRegEx, err := bashScript(f, file)
	if err != nil {
		log.Fatalf("unable to create bash script: %v", err)
	}

	if err := markdownFile(f, file, validationRegEx); err != nil {
		log.Fatalf("unable to create markdown file: %v", err)
	}
}

func markdownFile(contents []byte, fileName string, validationRegEx *regexp.Regexp) error {
	// create new markdown file based on name of source file
	out, err := os.Create(strings.Replace(fileName, ".source", "", -1))
	if err != nil {
		return err
	}
	defer out.Close()
	out.Chmod(0644)

	// convert tags to markdown syntax
	OpOpen := regexp.MustCompile(operationOpen)
	OpClose := regexp.MustCompile(operationClose)
	OpJSONOpen := regexp.MustCompile(operationJSONOpen)
	OpJSONClose := regexp.MustCompile(operationJSONClose)

	contents = OpOpen.ReplaceAll(contents, []byte("```"))
	contents = OpClose.ReplaceAll(contents, []byte("```"))
	contents = OpJSONOpen.ReplaceAll(contents, []byte("```json"))
	contents = OpJSONClose.ReplaceAll(contents, []byte("```"))
	contents = validationRegEx.ReplaceAll(contents, nil)
	out.Write(contents)

	return nil
}

func bashScript(contents []byte, fileName string) (*regexp.Regexp, error) {
	// create bash script file based on name of source file
	scriptFileName := fmt.Sprintf("%s%s", strings.Replace(fileName, ".md.source", "", -1), ".sh")
	script, err := os.Create(scriptFileName)
	if err != nil {
		return nil, err
	}
	defer script.Close()

	script.Chmod(0644)

	// todo(braskin): figure out a way to make the script template dynamic
	scriptTemplate, err := ioutil.ReadFile("script_template.txt")
	if err != nil {
		return nil, err
	}

	script.Write(scriptTemplate)

	operationRegEx := regexp.MustCompile(operationRegEx)
	validationRegEx := regexp.MustCompile(validationRegEx)

	// find all of the operations in the source file
	matches := operationRegEx.FindAll(contents, 100)

	for _, match := range matches {
		// find any validations
		validationRaw := validationRegEx.Find(match)

		// write operations to script
		match = validationRegEx.ReplaceAll(match, nil)
		operation := removeTags(string(match), []string{operationOpen, operationClose, operationJSONOpen, operationJSONClose})
		script.WriteString(operation)

		// write validations to script
		if len(validationRaw) > 0 {
			validation := removeTags(string(validationRaw), []string{validationOpen, validationClose})
			script.WriteString(validation)
		}
	}

	return validationRegEx, nil
}

func removeTags(text string, tagNames []string) string {
	for _, tag := range tagNames {
		text = strings.Replace(text, tag, "", -1)
	}
	return text
}
