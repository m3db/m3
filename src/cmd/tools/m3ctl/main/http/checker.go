package http

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func checkForAndHandleError(url string, resp *http.Response) {
	log.Printf("resp.StatusCode:%d:\n", resp.StatusCode)
	if resp.StatusCode > 299 {
		dat, _ := ioutil.ReadAll(resp.Body)
		if dat != nil {
			fmt.Println(string(dat))
		}
		log.Fatalf("error from m3db:%s:url:%s:", resp.Status, url)
	}
}
