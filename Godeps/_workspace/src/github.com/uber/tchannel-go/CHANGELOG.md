Changelog
=========

# 1.0.5

* Use `context.Context` storage for headers so `thrift.Context` and
  `tchannel.ContextWithHeaders` can be passed to functions that use
  `context.Context`, and have them retain headers.
* `thrift.Server` allows a custom factory to be used for `thrift.Context`
  creation based on the underlying `context.Context` and headers map.
* Store goroutine stack traces on channel creation that can be accessed
  via introspection.

# 1.0.4

* Improve handling of network failures during pending calls. Previously, calls
  would timeout, but now they fail as soon as the network failure is detected.
* Remove ephemeral peers with closed outbound connections.
* #233: Ensure errors returned from Thrift handlers have a non-nil value.
* #228: Add registered methods to the introspection output.
* Add ability to set a global handler for a SubChannel.

# 1.0.3

* Improved performance when writing Thrift structs
* Make closing message exchanges less disruptive, changes a panic due to
  closing a channel twice to an error log.
* Introspection now includes information about all channels created
  in the current process.

# 1.0.2

* Extend the `ContextBuilder` API to support setting the transport-level
  routing delegate header.
* Set a timeout when making new outbound connections to avoid hanging.
* Fix for #196: Make the initial Hyperbahn advertise more tolerant of transient
  timeouts.
* Assorted logging and test improvements.

# 1.0.1

* Bug fix for #181: Shuffle peers on PeerList.Add to avoid biases in peer
  selection.
* Peers can now be removed using PeerList.Remove.
* Add ErrorHandlerFunc to create raw handlers that return errors.
* Retries try to avoid previously selected hosts, rather than just the
  host:port.
* Create an ArgReader interface (which is an alias for io.ReadCloser) for
  symmetry with ArgWriter.
* Add ArgReadable and ArgWritable interfaces for the common methods between
  calls and responses.
* Expose Thrift binary encoding methods (thrift.ReadStruct, thrift.WriteStruct,
  thrift.ReadHeaders, thrift.WriteHeaders) so callers can easily send Thrift
  payloads over the streaming interface.

# 1.0.0

* First stable release.
* Support making calls with JSON, Thrift or raw payloads.
* Services use thrift-gen, and implement handlers with a `func(ctx, arg) (res,
  error)` signature.
* Support retries.
* Peer selection (peer heap, prefer incoming strategy, for use with Hyperbahn).
* Graceful channel shutdown.
* TCollector trace reporter with sampling support.
* Metrics collection with StatsD.
* Thrift support, including includes.
