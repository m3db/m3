package main

import (
	"github.com/m3db/m3coordinator/services/m3coordinator/httpd"
	"net/http"
	"fmt"
	"os"
)

func main() {
	handler, err := httpd.NewHandler()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to set up handlers, got error %v\n", err)
		os.Exit(1)
	}

	handler.Logger.Info("Starting server")
	handler.RegisterRoutes()
	http.ListenAndServe(":1234", handler.Router)

}
