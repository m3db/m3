package handler

import (
	"net/http"
)

// Error will server an HTTP error
func Error(w http.ResponseWriter, err error, code int) {
	http.Error(w, err.Error(), code)
}
