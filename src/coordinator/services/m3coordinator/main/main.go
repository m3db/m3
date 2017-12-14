package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/m3db/m3coordinator/services/m3coordinator/httpd"
	"github.com/m3db/m3coordinator/util/logging"
)

func main() {
	logging.InitWithCores(nil)
	handler, err := httpd.NewHandler()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to set up handlers, got error %v\n", err)
		os.Exit(1)
	}
	handler.RegisterRoutes()

	logger := logging.WithContext(context.TODO())
	defer logger.Sync()
	logger.Info("Starting server")
	http.ListenAndServe(":1234", handler.Router)

}
