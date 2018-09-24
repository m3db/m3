package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

const (
	operationRegEx  = `(?s)<operation>.*?</operation>|(?s)<operation_json>.*?</operation>`
	validationRegEx = `(?s)<validation>.*?</validation>`

	operationOpen   = `<operation>`
	operationClose  = `</operation>`
	operationJSON   = `<operation_json>`
	validationOpen  = `<validation>`
	validationClose = `</validation>`

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
		panic(err)
	}

	// create bash script file based on name of source file
	scriptFileName := fmt.Sprintf("%s%s", strings.Replace(file, ".md.source", "", -1), ".sh")
	script, err := os.Create(scriptFileName)
	if err != nil {
		panic(err)
	}
	defer script.Close()

	script.Chmod(0644)
	script.WriteString("#!/usr/bin/env bash \n")

	operationRegEx := regexp.MustCompile(operationRegEx)
	validationRegEx := regexp.MustCompile(validationRegEx)

	// find all of the operations in the source file
	matches := operationRegEx.FindAll(f, 100)

	for _, match := range matches {
		// find any validations
		validationRaw := validationRegEx.Find(match)

		// write operations to script
		match = validationRegEx.ReplaceAll(match, nil)
		operation := removeTags(string(match), []string{operationOpen, operationClose, operationJSON})
		script.WriteString(operation)

		// write validations to script
		if len(validationRaw) > 0 {
			validation := removeTags(string(validationRaw), []string{validationOpen, validationClose})
			script.WriteString(validation)
		}
	}

	// create new markdown file based on name of source file
	out, err := os.Create(strings.Replace(file, ".source", "", -1))
	if err != nil {
		panic(err)
	}
	defer out.Close()
	out.Chmod(0644)

	// convert tags to markdown syntax
	OpOpen := regexp.MustCompile(operationOpen)
	OpClose := regexp.MustCompile(operationClose)
	OpJSONOpen := regexp.MustCompile(operationJSON)

	f = OpOpen.ReplaceAll(f, []byte("```"))
	f = OpClose.ReplaceAll(f, []byte("```"))
	f = OpJSONOpen.ReplaceAll(f, []byte("```json"))
	f = validationRegEx.ReplaceAll(f, nil)
	out.Write(f)
}

func removeTags(text string, tagNames []string) string {
	for _, tag := range tagNames {
		text = strings.Replace(text, tag, "", -1)
	}
	return text
}
