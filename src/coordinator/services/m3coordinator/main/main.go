package main

import (
	"github.com/m3db/m3coordinator/services/m3coordinator/httpd"
	"net/http"
)

func main() {
	handler := httpd.NewHandler()
	handler.Logger.Info("Starting server")
	handler.RegisterRoutes()
	http.ListenAndServe(":1234", handler.Router)

}
