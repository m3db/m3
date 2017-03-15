package cmd

import (
	"fmt"
	"time"

	"github.com/m3db/m3nsch"

	xerrors "github.com/m3db/m3x/errors"
	"github.com/spf13/pflag"
)

type cliWorkload struct {
	m3nsch.Workload
	baseTimeOffset time.Duration
}

func (w *cliWorkload) validate() error {
	var multiErr xerrors.MultiError
	if w.baseTimeOffset >= time.Duration(0*time.Second) {
		multiErr = multiErr.Add(fmt.Errorf("basetime-offset must be negative"))
	}
	if w.Cardinality <= 0 {
		multiErr = multiErr.Add(fmt.Errorf("cardinality must be a positive integer"))
	}
	if w.IngressQPS <= 0 {
		multiErr = multiErr.Add(fmt.Errorf("ingress-qps must be a positive integer"))
	}
	if w.Namespace == "" {
		multiErr = multiErr.Add(fmt.Errorf("namespace must be set"))
	}
	return multiErr.FinalError()
}

func (w *cliWorkload) toM3nschWorkload() m3nsch.Workload {
	w.BaseTime = time.Now().Add(w.baseTimeOffset)
	return w.Workload
}

func registerWorkloadFlags(flags *pflag.FlagSet, workload *cliWorkload) {
	flags.DurationVarP(&workload.baseTimeOffset, "basetime-offset", "b", -2*time.Minute,
		`offset from current time to use for load, e.g. -2m, -30s`)
	flags.StringVarP(&workload.MetricPrefix, "metric-prefix", "p", "m3nsch_",
		`prefix added to each metric`)
	flags.StringVarP(&workload.Namespace, "namespace", "n", "testmetrics",
		`target namespace`)
	flags.IntVarP(&workload.Cardinality, "cardinality", "c", 10000,
		`aggregate workload cardinality`)
	flags.IntVarP(&workload.IngressQPS, "ingress-qps", "i", 1000,
		`aggregate workload ingress qps`)
}
