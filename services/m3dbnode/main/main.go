// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"path/filepath"
	// pprof: for debug listen server if configured
	_ "net/http/pprof"
	"os"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/services/m3dbnode/server"
	coordinator "github.com/m3db/m3db/src/coordinator/services/m3coordinator/server"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	dbOpts, coordOpts, err := parseFlags()
	if err != nil {
		os.Exit(1)
	}

	clientCh := make(chan client.Client)
	dbOpts.ClientBootstrapCh = clientCh
	server.Run(dbOpts)

	coordOpts.DBClient = <-clientCh
	coordinator.Run(coordOpts)
}

func parseFlags() (server.RunOptions, coordinator.RunOptions, error) {
	dbOpts := server.RunOptions{}
	coordOpts := coordinator.RunOptions{}
	a := kingpin.New(filepath.Base(os.Args[0]), "M3DB")

	a.Version("1.0")

	a.HelpFlag.Short('h')

	a.Flag("f", "M3DB configuration file path.").
		Default("db.yml").StringVar(&dbOpts.ConfigFile)

	a.Flag("config.file", "M3Coordinator configuration file path.").
		Default("coordinator.yml").StringVar(&coordOpts.ConfigFile)

	a.Flag("query.port", "Address to listen on.").
		Default("0.0.0.0:7201").StringVar(&coordOpts.ListenAddress)

	a.Flag("query.timeout", "Maximum time a query may take before being aborted.").
		Default("2m").DurationVar(&coordOpts.QueryTimeout)

	a.Flag("query.max-concurrency", "Maximum number of queries executed concurrently.").
		Default("20").IntVar(&coordOpts.MaxConcurrentQueries)

	a.Flag("rpc.enabled", "True enables remote clients.").
		Default("false").BoolVar(&coordOpts.RPCEnabled)

	a.Flag("rpc.port", "Address which the remote gRPC server will listen on for outbound connections.").
		Default("0.0.0.0:7288").StringVar(&coordOpts.RPCAddress)

	a.Flag("rpc.remotes", "Address which the remote gRPC server will listen on for outbound connections.").
		Default("[]").StringsVar(&coordOpts.Remotes)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		a.Usage(os.Args[1:])
		return server.RunOptions{}, coordinator.RunOptions{}, err
	}

	return dbOpts, coordOpts, nil
}
