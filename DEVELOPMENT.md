# Development Guide

M3 is a large, active codebase. This guide is for all potential code
contributors, designed to make it easier to get started with contributing to M3.

## Setup Development Environment

Fork <https://github.com/m3db/m3> to your own user or org account on GitHub.

Clone the fork:

```bash
mkdir -p $GOPATH/src/github.com/m3db
cd $GOPATH/src/github.com/m3db
git clone git@github.com:<fork_user_or_org>/m3.git
```

**Note**: You must push all commits/branches/etc to the fork, not to the upstream repository.

Verify that `git clone` set the repository's upstream:

```shell
$ git remote -v
origin  git@github.com:<fork_user_or_org>/m3.git (fetch)
origin  git@github.com:<fork_user_or_org>/m3.git (push)
```

Install dependencies:

```bash
cd m3
make install-vendor-m3
```

You can build each M3 component using `make` and the relevant task

-   A single combined database and coordinator node ideal for local development: `make m3dbnode`
-   An M3 Coordinator node: `make m3coordinator`
-   An M3 Aggregator node: `make m3aggregator`
-   An M3 Query node: `make m3query`

<!-- TODO: Why? What is this? Why do I need it? -->

## Running the M3 Stack Locally

Follow the instructions in [this README][local-readme].

[local-readme]: ./scripts/development/m3_stack/README.md

## Updating Mocks and Generated Files

<!-- TODO: What might these be? -->

If changes require updates to mocks or other generated files, make sure to
update those files. There are `make` targets to help with generation:

-   Mocks: `make mock-gen`
-   Protobuf: `make proto-gen` (Requires Docker)
-   Thrift: `make thrift-gen` (Requires Docker)

Don't forget to account for changes to generated files in tests.

## Scoping Pull Requests

Inspired by Phabricator's article about
[Recommendations on Revision Control][phab-one-idea], and particularly
because pull requests tend to be squash-merged, try to keep PRs focused
on one idea (or the minimal number of ideas for the change to be viable).

The advantages of smaller PRs are:

-   Quicker to write + quicker to review = faster iteration
-   Easier to spot errors
-   Less cognitive overhead (and net time invested) for reviewers due to
    reduced scope
-   Avoids scope creep
-   Clearly establishes that all parts of the PR are related

Because of this, contributors are encouraged to keep PRs as small as
possible. Given that this does introduce some developer overhead (e.g.
needing to manage more PRs), _how_ small is unspecified; reviewers
can request breaking PRs down further as necessary, and contributors
should work with reviewers to find the right balance.

[phab-one-idea]: https://secure.phabricator.com/book/phabflavor/article/recommendations_on_revision_control/#one-idea-is-one-commit

## Testing Changes

M3 has an extensive test suite to ensure that we're able to validate changes. You can find more notes about the testing strategies employed in [TESTING][TESTING.md].

While the CI suite runs all tests before allowing the merging of pull requests
, developers should test their changes during development and before
pushing updates.

To test code, use `go test` from the root directory:

```shell
go test -v ./src/...
```

The time required to run the entire suite has increased significantly. We recommend only running tests for both (1) any packages with changes and (2) any packages that
import the changed packages. An example of this is:

```bash
changed=$(git diff --name-only HEAD^ HEAD | xargs -I {} dirname {} | sort | uniq)
for pkg in $changed; do
  affected=$(grep -r "$pkg" ./src | cut -d: -f1 | grep -v mock | xargs -I{} dirname {} | sort | uniq)
  go test -v -race "./$pkg" $affected
done
```

Contributors are free to do whatever due diligence that they feel helps them to
be most productive. Our only request is that contributors don't use CI jobs as a first-pass filter to determine whether a change is sane or not.

Once tests are passing locally, push to a new remote branch to create a pull
request, or push to an existing branch to update a pull request. If the CI
suite reports any errors, attempt to reproduce failures locally and fix them
before continuing.

For larger or more intensive tests (e.g. "big" unit tests, integration tests),
you may need additional build tags to scope the tests down to a smaller
subset, for example:

```shell
# example integration test
$ go test ./integration -tags integration -run TestIndexBlockRotation -v

# example big unit test
$ go test -tags big ./services/m3dbnode/main -run TestIndexEnabledServer -v
```

## Code Review

Please follow the following guidelines when submitting or reviewing pull
requests:

-   Merging is blocked on approval by 1 or more users with write access. We
    recommend 2+ for large, complex, or nuanced changes.
-   Pull requests should contain clear descriptions and context per the
    pull request template.
-   The [Technical Steering Committee (TSC)][GOVERNANCE.md] must approve breaking changes  
    under the current versioning guarantees (see [COMPATIBILITY][COMPATIBILITY.md]).
-   You should follow the [STYLEGUIDE][STYLEGUIDE.md] to the extent reasonable
    within the scope of the pull request.
-   You should validate changed codepaths should by unit tests that cover at least one
    nominal case and one error case.
-   Pull requests are only merged with a green build. Don't merge with
    build failures, even if they're known and unrelated.
-   Flaky or otherwise unreliable CI failures count as hard failures. Commit fixes
    or skips for flaky tests or other failures first.

Your pull request is most likely to be accepted if it:

-   Includes tests for new functionality.
-   Follows the guidelines in [Effective
    Go](https://golang.org/doc/effective_go.html) and the [Go team's common code
    review comments](https://github.com/golang/go/wiki/CodeReviewComments).
-   Has a [good commit
    message](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).
-   It prefixes the name of the pull request with the component you are updating in the format "[component] Change title" (for example "[dbnode] Support out of order writes").

## Updating the CHANGELOG

After a pull request is merged, summarize significant changes in the
[CHANGELOG][CHANGELOG.md]. Since the you don't know the PR number ahead of time, 
do this as a subsequent commit after the merged PR (though you can prepare the 
CHANGELOG PR in tandem with the referenced PR).

The format of the CHANGELOG entry should be:

```text
- TYPE **COMPONENT**: Description (#PR_NUMBER)
```

Omit the `TYPE` should if it's a feature. If the change isn't a
feature, `TYPE` should be either `FIX` or `PERF`. Add new types of changes
to this document before used if you can't categorize them by the
existing types.

Add new CHANGELOG entries to the current "unreleased" section at
the top of the CHANGELOG as long as they're within the scope of that potential
version. If no unreleased section exists, add one using the
appropriate proposed semver. If an unreleased section exists but the new change
requires a different semver change (e.g. the unreleased version is a patch
version bump, but the new change requires a minor version bump), update the version
on the existing section. See [COMPATIBILITY][COMPATIBILITY.md]
for more information on versioning.

An example CHANGELOG addition:

```text
# 0.4.5 (unreleased)

- FIX **DB**: Index data race in FST Segment reads (#938)
```

## Cutting a Release

1.  Check you have a GitHub API access token with the `repo` scope
2.  Checkout the commit you want to release (all releases must be on master)
3.  Create a tag with the version you want to release
    -   E.g. `git tag -a v0.7.0 -m "v0.7.0"`
    -   Read [COMPATIBILITY][COMPATIBILITY.md] for semver information.
4.  Push the tag
    -   E.g. `git push origin v0.7.0`
5.  Run `make GITHUB_TOKEN=<GITHUB_API_ACCESS_TOKEN> release`
6.  Update [CHANGELOG.md](CHANGELOG.md) and commit it to master
7.  Copy and paste the text from [CHANGELOG.md](CHANGELOG.md) into the release notes [on GitHub][releases].

[CHANGELOG.md]: https://github.com/m3db/m3/blob/master/CHANGELOG.md

[COMPATIBILITY.md]: https://github.com/m3db/m3/blob/master/COMPATIBILITY.md

[GOVERNANCE.md]: https://github.com/m3db/m3/blob/master/GOVERNANCE.md

[STYLEGUIDE.md]: https://github.com/m3db/m3/blob/master/STYLEGUIDE.md

[TESTING.md]: https://github.com/m3db/m3/blob/master/TESTING.md

[m3db.io]: https://m3db.io/

[releases]: https://github.com/m3db/m3/releases
