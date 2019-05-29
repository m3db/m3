// Copyright (c) 2018 Uber Technologies, Inc.
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

package handler

const (
	// WarningsHeader is the M3 warnings header when to display a warning to a user.
	WarningsHeader = "M3-Warnings"

	// RetryHeader is the M3 retry header to display when it is safe to retry.
	RetryHeader = "M3-Retry"

	// ServedByHeader is the M3 query storage execution breakdown.
	ServedByHeader = "M3-Storage-By"

	// DeprecatedHeader is the M3 deprecated header.
	DeprecatedHeader = "M3-Deprecated"

	// LimitMaxSeriesHeader is the M3 limit timeseries header that limits
	// the number of time series returned by each storage node.
	LimitMaxSeriesHeader = "M3-Limit-Max-Series"

	// DefaultServiceEnvironment is the default service ID environment.
	DefaultServiceEnvironment = "default_env"
	// DefaultServiceZone is the default service ID zone.
	DefaultServiceZone = "embedded"

	// HeaderClusterEnvironmentName is the header used to specify the environment
	// name.
	HeaderClusterEnvironmentName = "Cluster-Environment-Name"
	// HeaderClusterZoneName is the header used to specify the zone name.
	HeaderClusterZoneName = "Cluster-Zone-Name"
	// HeaderDryRun is the header used to specify whether this should be a dry
	// run.
	HeaderDryRun = "Dry-Run"
	// HeaderForce is the header used to specify whether this should be a forced operation.
	HeaderForce = "Force"
)
