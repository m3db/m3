M3DB Testing Patterns
=====================

`m3db` uses a combination of testing strategies, they're listed below for completeness.

(1) Unit tests
These are package local tests where we use mocks/stubs to ensure components interact with each other
as we expect.

These come in two flavors:
- white-box: i.e. the tests know about the internals of the components they are testing, and may modify them
  (by injecting functions, changing consts) for testing.
- black-box: i.e. when the tests do not know about the internals of the components they are testing, and rely
  on only the exported methods for testing.

(2) Property tests
We use a property testing library, [gopter] for generative tests. These allow us to specify input generators,
and ensure the invariants expected on the system hold.

These come in two flavors:
- Vanilla property tests: used when we're testing `pure` functions (i.e. side-effect free).
- System under test: used when we're testing stateful systems.

(3)
