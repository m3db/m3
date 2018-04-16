M3DB Testing Patterns
=====================

`m3db` uses a combination of testing strategies, they're listed below for completeness.

(1) Unit tests
These are package local tests where we use mocks/stubs to ensure components interact with each other
as we expect.

These come in two flavours:
- white-box: i.e. the tests know about the internals of the components they are testing, and may modify them
  (by injecting functions, changing consts) for testing.
- black-box: i.e. when the tests do not know about the internals of the components they are testing, and rely
  on only the exported methods for testing.

These are all run by the CI for every push, with race detection enabled, and code coverage within the package
being tested.

(2) Property tests
We use a property testing library, [gopter] for generative tests. These allow us to specify input generators,
and ensure the invariants expected on the system hold.

[gopter]: https://godoc.org/github.com/leanovate/gopter

These come in two flavours:
- Vanilla property tests: used when we're testing `pure` functions (i.e. side-effect free).
- System under test: used when we're testing stateful systems. The allow generation of input state,
  and the methods use to transition the state of the system.

These are all run by the CI for every push, with race detection enabled, and code coverage within the package
being tested.

(3) Big Unit Tests
These are a specialized version of the unit tests that are marked with the build tag `big`. They are heavier weight
unit tests for which we disable race detection.

These are all run by the CI for every push, with race detection disabled, and code coverage measured across all
m3db packages.

(4) Integration tests
These tests are heavier weight tests that spin up one or more m3db DB's within a single process and test
interactions with each other, and other components (e.g. etcd, read/write quorum).

These are all run by the CI for every push, with race detection disabled, and code coverage measured across all
m3db packages.

(5) DTests
TODO

