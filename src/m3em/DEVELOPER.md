Developer Notes
===============

Utility Packages:
  `build` - utility package to represent build/confs (public; tiny)
  `os` - utility package for FileIteration and Process Lifecycle management (private)
  `cluster` - is a collection of instances along with corresponding placement alteration (public)

Packages for client<->server communications, life-cycle maintenance, event propagation.
  `node` - main construct is ServiceNode, which wraps services.PlacementInstance (public)
  `generated` - grpc schemas for Operator, and Heartbeat services (private)
  `agent` - the server side Operator implementation, client side Heartbeat implementation (private)
  `services` - m3em_agent main (private)
  `integration` - currently has tests for file transfer, process lifecycle, and heartbeating (private)