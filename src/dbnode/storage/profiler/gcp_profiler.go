/*
 * // Copyright (c) 2021 Uber Technologies, Inc.
 * //
 * // Permission is hereby granted, free of charge, to any person obtaining a copy
 * // of this software and associated documentation files (the "Software"), to deal
 * // in the Software without restriction, including without limitation the rights
 * // to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * // copies of the Software, and to permit persons to whom the Software is
 * // furnished to do so, subject to the following conditions:
 * //
 * // The above copyright notice and this permission notice shall be included in
 * // all copies or substantial portions of the Software.
 * //
 * // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * // IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * // FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * // AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * // LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * // OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * // THE SOFTWARE.
 */

package profiler

import (
	"bytes"
	"context"
	"runtime/pprof"

	gcemd "cloud.google.com/go/compute/metadata"
	"google.golang.org/api/option"
	gtransport "google.golang.org/api/transport/grpc"
	pb "google.golang.org/genproto/googleapis/devtools/cloudprofiler/v2"

	"github.com/m3db/m3/src/x/instrument"
)

const (
	apiAddr = "cloudprofiler.googleapis.com:443"
	scope   = "https://www.googleapis.com/auth/monitoring.write"
)

// GcpProfile is gcp profile.
type GcpProfile struct {
	serviceName ServiceName
	projectID   string
	zone        string
	version     string
	payload     bytes.Buffer
}

// NewGcpProfile returns new gcp profile.
func NewGcpProfile(serviceName ServiceName) *GcpProfile {
	return &GcpProfile{
		serviceName: serviceName,
		version:     instrument.Version,
	}
}

// StartCPUProfile starts new cpu profile.
func (gp *GcpProfile) StartCPUProfile() {
	if err := pprof.StartCPUProfile(&gp.payload); err != nil {
		// if cpu profile already started by some other code, stop it and start again
		pprof.StopCPUProfile()
		gp.StartCPUProfile()
	}
}

// StopCPUProfile stops already started cpu profile and uploads it to gcp.
func (gp *GcpProfile) StopCPUProfile() error {
	pprof.StopCPUProfile()
	return gp.uploadProfile(pb.ProfileType_CPU)
}

// WriteHeapProfile writes new heap profile and uploads it to gcp.
func (gp *GcpProfile) WriteHeapProfile() error {
	gp.payload.Reset()
	if err := pprof.WriteHeapProfile(&gp.payload); err != nil {
		return err
	}
	return gp.uploadProfile(pb.ProfileType_HEAP)
}

func (gp *GcpProfile) uploadProfile(profileType pb.ProfileType) error {
	if gp.zone == "" {
		gcZone, err := gcemd.Zone()
		if err != nil {
			return err
		}
		gp.zone = gcZone
	}

	if gp.projectID == "" {
		gcProjectID, err := gcemd.ProjectID()
		if err != nil {
			return err
		}
		gp.projectID = gcProjectID
	}

	ctx := context.Background()
	conn, err := gtransport.Dial(ctx,
		option.WithEndpoint(apiAddr),
		option.WithScopes(scope))
	defer func() {
		_ = conn.Close()
	}()
	if err != nil {
		return err
	}
	client := pb.NewProfilerServiceClient(conn)

	req := &pb.CreateOfflineProfileRequest{
		Parent: "projects/" + gp.projectID,
		Profile: &pb.Profile{
			ProfileType: profileType,
			Deployment: &pb.Deployment{
				ProjectId: gp.projectID,
				Target:    gp.serviceName.String(),
				Labels: map[string]string{
					"zone":    gp.zone,
					"version": gp.version,
				},
			},
			ProfileBytes: gp.payload.Bytes(),
		},
	}

	_, err = client.CreateOfflineProfile(ctx, req)
	return err
}
