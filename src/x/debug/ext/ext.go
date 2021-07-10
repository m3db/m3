// Package extdebug contains helpers for more detailed debug information
package extdebug

import (
	"fmt"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/placementhandler"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/x/debug"
	"github.com/m3db/m3/src/x/instrument"
)

// NewPlacementAndNamespaceZipWriterWithDefaultSources returns a zipWriter with the following
// debug sources already registered: CPU, heap, host, goroutines, namespace and placement info.
func NewPlacementAndNamespaceZipWriterWithDefaultSources(
	cpuProfileDuration time.Duration,
	clusterClient clusterclient.Client,
	placementsOpts placementhandler.HandlerOptions,
	services []handleroptions.ServiceNameAndDefaults,
	instrumentOpts instrument.Options,
) (debug.ZipWriter, error) {
	zw, err := debug.NewZipWriterWithDefaultSources(cpuProfileDuration,
		instrumentOpts)
	if err != nil {
		return nil, err
	}

	if clusterClient != nil {
		nsSource, err := NewNamespaceInfoSource(clusterClient, services, instrumentOpts)
		if err != nil {
			return nil, fmt.Errorf("could not create namespace info source: %w", err)
		}

		err = zw.RegisterSource("namespace.json", nsSource)
		if err != nil {
			return nil, fmt.Errorf("unable to register namespaceSource: %w", err)
		}

		for _, service := range services {
			placementInfoSource, err := NewPlacementInfoSource(service,
				placementsOpts, instrumentOpts)
			if err != nil {
				return nil, fmt.Errorf("unable to create placementInfoSource: %w", err)
			}
			fileName := fmt.Sprintf("placement-%s.json", service.ServiceName)
			err = zw.RegisterSource(fileName, placementInfoSource)
			if err != nil {
				return nil, fmt.Errorf("unable to register placementSource: %w", err)
			}
		}
	}

	return zw, nil
}
