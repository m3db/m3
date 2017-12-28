Package integration contains integration tests for the collector.

The directory is structured as follows:
* msgpack/: simple msgpack server receiving traffic sent from the collector for integration testing.
* client.go: wrapper client for testing TCP connections.
* data.go: data structures and utility functions for test data generation and validation.
* defaults.go: default values for various testing parameters.
* integration.go: general utility functions.
* options.go: configurable options for tuning test parameters.
* setup.go: general initialization logic for test setup.
* *_test.go: integration tests.
