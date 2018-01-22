package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/m3db/m3coordinator/services/m3coordinator/config"
	"github.com/m3db/m3coordinator/services/m3coordinator/httpd"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/util/logging"
	xconfig "github.com/m3db/m3x/config"

	"go.uber.org/zap"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

type m3config struct {
	configFile           string
	listenAddress        string
	maxConcurrentQueries int
	queryTimeout         time.Duration
}

func main() {
	rand.Seed(time.Now().UnixNano())

	flags := parseFlags()

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, flags.configFile); err != nil {
		fmt.Fprintf(os.Stderr, "unable to load %s: %v", flags.configFile, err)
		os.Exit(1)
	}

	logging.InitWithCores(nil)

	storage := local.NewStorage()
	handler, err := httpd.NewHandler(storage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to set up handlers, got error %v\n", err)
		os.Exit(1)
	}
	handler.RegisterRoutes()

	logger := logging.WithContext(context.TODO())
	defer logger.Sync()
	logger.Info("Starting server", zap.String("address", flags.listenAddress))
	http.ListenAndServe(flags.listenAddress, handler.Router)
}

func parseFlags() *m3config {
	cfg := m3config{}
	a := kingpin.New(filepath.Base(os.Args[0]), "M3Coordinator")

	a.Version("1.0")

	a.HelpFlag.Short('h')

	a.Flag("config.file", "M3Coordinator configuration file path.").
		Default("coordinator.yml").StringVar(&cfg.configFile)

	a.Flag("query.port", "Address to listen on.").
		Default("0.0.0.0:7201").StringVar(&cfg.listenAddress)

	a.Flag("query.timeout", "Maximum time a query may take before being aborted.").
		Default("2m").DurationVar(&cfg.queryTimeout)

	a.Flag("query.max-concurrency", "Maximum number of queries executed concurrently.").
		Default("20").IntVar(&cfg.maxConcurrentQueries)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing commandline arguments, got error %v\n", err)
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	return &cfg
}
