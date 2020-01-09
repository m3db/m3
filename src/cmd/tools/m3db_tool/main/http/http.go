package http

import (
	"fmt"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

const timeout = time.Duration(5 * time.Second)

func DoGet(url string, logger *zap.SugaredLogger, getter func(reader io.Reader, logger *zap.SugaredLogger)) {
	logger.Debugf("DoGet:url:%s:\n", url)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get(url)
	if err != nil {
		logger.Fatal(err)
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	checkForAndHandleError(url, resp, logger)
	getter(resp.Body, logger)
}

func DoPost(url string, data io.Reader, logger *zap.SugaredLogger, getter func(reader io.Reader, logger *zap.SugaredLogger)) {
	logger.Debugf("DoPost:url:%s:\n", url)
	client := &http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequest(http.MethodPost, url, data)
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		logger.Fatal(err)

	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	checkForAndHandleError(url, resp, logger)
	getter(resp.Body, logger)
}

func DoDelete(url string, logger *zap.SugaredLogger, getter func(reader io.Reader, logger *zap.SugaredLogger)) {
	logger.Debugf("DoDelete:url:%s:\n", url)
	client := &http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		logger.Fatal(err)
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	checkForAndHandleError(url, resp, logger)
	getter(resp.Body, logger)
}

func DoDump(in io.Reader, log *zap.SugaredLogger) {
	dat, err := ioutil.ReadAll(in)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(dat))
}

func checkForAndHandleError(url string, resp *http.Response, log *zap.SugaredLogger) {
	log.Debugf("resp.StatusCode:%d:\n", resp.StatusCode)
	if resp.StatusCode > 299 {
		dat, _ := ioutil.ReadAll(resp.Body)
		if dat != nil {
			fmt.Println(string(dat))
		}
		log.Fatalf("error from m3db:%s:url:%s:", resp.Status, url)
	}
}
