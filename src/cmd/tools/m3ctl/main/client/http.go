package client

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"go.uber.org/zap"
)

const timeout = time.Duration(5 * time.Second)

func DoGet(url string, getter func(reader io.Reader, zl *zap.SugaredLogger), zl *zap.SugaredLogger) {
	zl.Infof("DoGet:url:%s:\n", url)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get(url)
	if err != nil {
		zl.Fatal(err)
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	checkForAndHandleError(url, resp, zl)
	getter(resp.Body, zl)
}

func DoPost(url string, data io.Reader, getter func(reader io.Reader, zl *zap.SugaredLogger), zl *zap.SugaredLogger) {
	zl.Infof("DoPost:url:%s:\n", url)
	client := &http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequest(http.MethodPost, url, data)
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		zl.Fatal(err)

	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	checkForAndHandleError(url, resp, zl)
	getter(resp.Body, zl)
}

func DoDelete(url string, getter func(reader io.Reader, zl *zap.SugaredLogger), zl *zap.SugaredLogger) {
	zl.Infof("DoDelete:url:%s:\n", url)
	client := &http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		zl.Fatal(err)
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	checkForAndHandleError(url, resp, zl)
	getter(resp.Body, zl)
}

func Dumper(in io.Reader, zl *zap.SugaredLogger) {
	dat, err := ioutil.ReadAll(in)
	if err != nil {
		zl.Fatal(err)
	}
	fmt.Println(string(dat))
}
