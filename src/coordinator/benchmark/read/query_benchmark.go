package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/m3db/m3coordinator/benchmark/common"
	"github.com/m3db/m3coordinator/services/m3coordinator/config"

	"github.com/m3db/m3db/client"
	xconfig "github.com/m3db/m3x/config"
)

var (
	m3dbClientCfg     string
	dataFile          string
	workers           int
	batch             int
	namespace         string
	memprofile        bool
	cpuprofile        bool
	timestampStartStr string
	timestampEndStr   string

	timestampStart time.Time
	timestampEnd   time.Time
)

func init() {
	flag.StringVar(&m3dbClientCfg, "m3db-client-config", "benchmark.yml", "used to create m3db client session")
	flag.StringVar(&dataFile, "data-file", "data.json", "input data for benchmark")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.IntVar(&batch, "batch", 5000, "Batch Size")
	flag.StringVar(&namespace, "namespace", "metrics", "M3DB namespace where to store result metrics")
	flag.StringVar(&timestampStartStr, "timestamp-start", "2016-01-01T00:00:00Z", "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", "2016-01-01T06:00:00Z", "Ending timestamp (RFC3339).")
	flag.BoolVar(&memprofile, "memprofile", false, "Enable memory profile")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "Enable cpu profile")
	flag.Parse()
}

func main() {
	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, m3dbClientCfg); err != nil {
		log.Fatalf("Unable to load %s: %v", m3dbClientCfg, err)
	}

	// Parse timestamps:
	var err error
	timestampStart, err = time.Parse(time.RFC3339, timestampStartStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampStart = timestampStart.UTC()
	timestampEnd, err = time.Parse(time.RFC3339, timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampEnd = timestampEnd.UTC()

	m3dbClientOpts := cfg.M3DBClientCfg
	m3dbClient, err := m3dbClientOpts.NewClient(client.ConfigurationParameters{}, func(v client.Options) client.Options {
		return v.SetWriteBatchSize(batch).SetWriteOpPoolSize(batch * 2)
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create m3db client, got error %v\n", err)
		os.Exit(1)
	}

	session, err := m3dbClient.NewSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create m3db client session, got error %v\n", err)
		os.Exit(1)
	}

	ids := make([]string, 0, common.MetricsLen)
	common.ConvertToM3(dataFile, workers, func(m *common.M3Metric) {
		ids = append(ids, m.ID)
	})

	start := time.Now()

	rawResults, err := session.FetchAll(namespace, ids, timestampStart, timestampEnd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to fetch metrics from m3db, got error %v\n", err)
		os.Exit(1)
	}

	end := time.Now()
	took := end.Sub(start)
	rate := float64(rawResults.Len()) / took.Seconds()

	fmt.Printf("returned %d timeseries in %fsec (mean values rate %f/sec)\n", rawResults.Len(), took.Seconds(), rate)
}
