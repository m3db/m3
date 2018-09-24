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
	starting   = `(?s)<operation>.*?</operation>`
	docTest    = `<operation>`
	docTestEnd = `</operation>`
	validation = `(?s)<validation>.*?</validation>`
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

	re := regexp.MustCompile(starting)
	validationRegEx := regexp.MustCompile(validation)

	matches := re.FindAll(f, 100)
	scriptFileName := fmt.Sprintf("%s%s", strings.Replace(file, ".md.source", "", -1), ".sh")
	script, err := os.Create(scriptFileName)
	if err != nil {
		panic(err)
	}

	defer script.Close()
	script.Chmod(0644)
	script.WriteString("#!/usr/bin/env bash \n")

	for _, match := range matches {
		fmt.Println(string(match))
		fmt.Println("***********")
		replaced := strings.Replace(string(match), "<doc_test>", "", -1)
		replaced = strings.Replace(replaced, "</doc_test>", "", -1)
		script.Write([]byte(replaced))
	}

	// create new markdown file without '.source'
	out, err := os.Create(strings.Replace(file, ".source", "", -1))
	if err != nil {
		panic(err)
	}
	defer out.Close()
	out.Chmod(0644)

	open := regexp.MustCompile(docTest)
	close := regexp.MustCompile(docTestEnd)

	f = open.ReplaceAll(f, []byte("```"))
	f = close.ReplaceAll(f, []byte("```"))
	f = validationRegEx.ReplaceAll(f, nil)
	out.Write(f)
}
