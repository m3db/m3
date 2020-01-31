package client

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

const timeout = time.Duration(5 * time.Second)

func DoGet(url string, getter func(reader io.Reader)) {
	log.Printf("DoGet:url:%s:\n", url)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	checkForAndHandleError(url, resp)
	getter(resp.Body)
}

func DoPost(url string, data io.Reader, getter func(reader io.Reader)) {
	log.Printf("DoPost:url:%s:\n", url)
	client := &http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequest(http.MethodPost, url, data)
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)

	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	checkForAndHandleError(url, resp)
	getter(resp.Body)
}

func DoDelete(url string, getter func(reader io.Reader)) {
	log.Printf("DoDelete:url:%s:\n", url)
	client := &http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	checkForAndHandleError(url, resp)
	getter(resp.Body)
}

func Dumper(in io.Reader) {
	dat, err := ioutil.ReadAll(in)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(dat))
}
