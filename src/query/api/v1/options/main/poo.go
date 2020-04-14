package main

import (
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/gorilla/mux"
)

func YourHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Gorilla!\n"))
}

func PooHandler(w http.ResponseWriter, r *http.Request) {
	if r.Response != nil {
		fmt.Println("BEFORE", w.Header(), r.Response.Status, r.Response.StatusCode)
	} else {
		fmt.Println("nil response")
	}
	YourHandler(w, r)
	if r.Response != nil {
		fmt.Println("HEADERS", w.Header(), r.Response.Status, r.Response.StatusCode)
	} else {
		fmt.Println("nil responsefater", w.Header())
		fmt.Printf("%+v", r)
	}
	// w.Write([]byte("Poo!\n"))
}

func main() {
	r := mux.NewRouter()
	// Routes consist of a path and a handler function.
	r.HandleFunc("/route", YourHandler)
	r.HandleFunc("/routez", PooHandler)

	fmt.Println("starting", r.GetRoute("/route"))
	// Bind to a port and pass our router in
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprint(err))
	}

	url := fmt.Sprintf(":%d", listener.Addr().(*net.TCPAddr).Port)
	fmt.Println("STARTING ON URL", url)
	log.Fatal(http.Serve(listener, r))
}

// func (s *forwardHandler) ServeHTTP(_ http.ResponseWriter, r *http.Request) {
// 	require.NoError(s.t, r.ParseForm())
// 	fmt.Println("Serving forwarded handler:")
// 	fmt.Println()
// 	fmt.Println(r.Form)
// }

// func TestPromReadHandler(t *testing.T) {
// 	ctrl := xtest.NewController(t)
// 	defer ctrl.Finish()

// 	listener, err := net.Listen("tcp", ":0")
// 	require.NoError(t, err)

// 	forward := &forwardHandler{}
// }
