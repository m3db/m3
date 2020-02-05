package client

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"go.uber.org/zap"
)

func checkForAndHandleError(url string, resp *http.Response, zl *zap.SugaredLogger) {
	zl.Infof("resp.StatusCode:%d:\n", resp.StatusCode)
	if resp.StatusCode > 299 {
		dat, _ := ioutil.ReadAll(resp.Body)
		if dat != nil {
			fmt.Println(string(dat))
		}
		zl.Fatalf("error from m3db:%s:url:%s:", resp.Status, url)
	}
}
