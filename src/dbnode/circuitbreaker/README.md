# circuitbreakerfx
==================

This package is adapted from the monorepo circuitbreaker package (https://sg.uberinternal.com/code.uber.internal/uber-code/go-code/-/blob/src/code.uber.internal/rpc/circuitbreakerfx/README.md) for statsdex_query. We use the `Circuit` implementation from that package's `internal/` subpackage, but we can't use the rest of the YARPC bindings because query doesn't support YARPC yet.

This package's implementation aims to stay contract compatible with the original code (reuse the existing Circuit implementation and configuration without modifications, recreate existing metrics) to minimize future migration woes.
