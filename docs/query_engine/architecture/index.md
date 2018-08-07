# Architecture

**Please note:** This documentation is a work in progress and more detail is required.

## Overview

M3 Query and M3 Coordinator are written entirely in Go and can act as both a query engine for [M3DB](https://m3db.github.io/m3db/) and as a remote read/write endpoint for Prometheus and M3DB. To learn more about Prometheus's remote endpoints and storage, [see here](https://prometheus.io/docs/operating/integrations/#remote-endpoints-and-storage).
