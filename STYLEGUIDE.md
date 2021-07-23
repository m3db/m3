# M3 Coding Styleguide

M3's umbrella coding style guide is Uber's [Go Style Guide][uber-guide]. This
document is maintained as a superset of that guide, capturing any substantive,
intended departures from Uber's coding style. Where possible, code should follow
these guidelines to ensure consistent style and coding practices across the
codebase.

Above all else, this guide is intended to be a point of reference rather than
a sacred text. We maintain a style guide as a pragmatic tool that can be used to
avoid common issues and normalize code across many authors and the Go community.

New code should follow the style guide by default, preferring guidelines
established here or in Uber's guide over any conflicting, pre-existing
precedents in the M3 codebase. Ultimately, the hope is that the codebase
incrementally moves closer to the style guide with each change.

Since the M3 monorepo predated this style guide, reviewers should not expect
contributors to make unrelated or unreasonable style-based changes as part of
pull requests. However, when changing code that could reasonably be updated
to follow the guide, we prefer that those changes adopt the guidelines to avoid
sustaining or increasing technical debt. See DEVELOPMENT.md for more detail on
changes involving style.

[uber-guide]: https://github.com/uber-go/guide/blob/master/style.md

## Linting

Many guidelines are flagged by `go vet` or the other configured linters (see
[.golangci.yml][.golangci.yml]). Wherever possible, we prefer to use tooling to
enforce style to remove subjectivity or ambiguity. Linting is also a blocking
build for merging pull requests.

[.golangci.yml]: https://github.com/m3db/m3/blob/master/.golangci.yml

## Template

When adding to this guide, use the following template:

~~~
### Short sentence about the guideline.

Clearly (and succinctly) articulate the guideline and its rationale, including
any problematic counter-examples. Be intentional when using language like
"always", "never", etc, instead using words like "prefer" and "avoid" if the
guideline isn't a hard rule. If it makes sense, also include example code:

<table>
<thead><tr><th>Bad</th><th>Good</th></tr></thead>
<tbody>
<tr><td>

```go
goodExample := false
```

</td><td>

```go
goodExample := true
```

</td></tr>
<tr><td>
Description of bad code.
</td><td>
Description of good code.
</td></tr>
</tbody></table>
~~~

## Guidelines

### Export types carefully.

Types should only be exported if they must be used by multiple packages. This
applies also to adding new packages: a new package should only be added if it
will be imported by multiple packages. If a given type or package will only
initially be imported in one package, define those type(s) in that importing
package instead.

In general, it's harder to reduce surface area than it is to incrementally
increase surface area, and the former is a breaking change while the latter is
often not.

### Treat flaky tests like consistent failures.

Flaky tests add noise to code health signals, reduce trust in tests to be
representative of code behavior. Worse, flaky tests can be either false positive
or false negative, making it especially unclear as to whether or not a given
test passing or failing is good or bad. All of these reduce overall velocity
and/or reliability.

All tests discovered to be flaky should be immediately result in either (a) the
test being skipped because it is unreliable, or (b) master being frozen until
the test is fixed and proven to no longer be flaky.

### Do not expose experimental package types in non-experimental packages.

A package is only able to guarantee a level of maturity/stability that is the
lowest common denominator of all of its composing or transitively exported
types. Given a hypothetical scenario:

```go
package foo

type Bar {
  Baz xfoo.Baz
}
```

In this case, the stability of `foo.Bar` is purportedly guaranteed by package
`foo` being non-experimental, but since it transitively exposes `xfoo.Baz` as
part of `foo.Bar`, either (a) `xfoo.Baz` must implicitly adhere to versioning
compatibility guarantees or (b) `foo` can no longer be considered stable,
as any breaking change to `xfoo.Baz` will break `foo`.

This is spiritually similar to the
[Avoid Embedding Types In Public Structs][avoid-embedding-types] guidance, in
that it bleeds implementation and compatibility details in an inappropriate way.

This guidance also applies to any cases in which `internal` packages are used:
any `internal` type is essentially the same as an unexported type, meaning that
that type is only implicitly available to users.

[avoid-embedding-types]: https://github.com/uber-go/guide/blob/master/style.md#avoid-embedding-types-in-public-structs

<table>
<thead><tr><th>Bad</th><th>Good</th></tr></thead>
<tbody>
<tr><td>

```go
type NewConnectionFn func(
    channelName string, addr string, opts Options,
) (xclose.SimpleCloser, rpc.TChanNode, error)
```

</td><td>

```go
type NewConnectionFn func(
    channelName string, addr string, opts Options,
) (io.Closer, rpc.TChanNode, error)

// or

type SimpleCloser = func()

type NewConnectionFn func(
    channelName string, addr string, opts Options,
) (SimpleCloser, rpc.TChanNode, error)
```

</td></tr>
<tr><td>

`xclose.SimpleCloser` is part of `x/close`, an experimental package, but is
directly exposed as part of `src/dbnode/client.NewConnectionFn`.

</td><td>

The canonical `io.Closer` is used instead, or a type alias representing
`xclose.SimpleCloser` is used instead. Both options prevent leaking experimental
packages as part of non-experimental library APIs.

</td></tr>
</tbody></table>
