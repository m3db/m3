// Copyright (c) 2021 Uber Technologies, Inc.
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
			placementInfoSource, err := NewPlacementInfoSource(service, placementsOpts)
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
