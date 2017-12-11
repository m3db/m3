package main

import (
	"github.com/m3db/m3coordinator/services/m3coordinator/httpd"
	"net/http"
	"fmt"
	"os"
	"github.com/m3db/m3coordinator/util/logging"
	"context"
)

func main() {
	logging.InitWithCores(nil)
	handler, err := httpd.NewHandler()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to set up handlers, got error %v\n", err)
		os.Exit(1)
	}
	handler.RegisterRoutes()

	logging.WithContext(context.TODO()).Info("Starting server")
	http.ListenAndServe(":1234", handler.Router)

}
