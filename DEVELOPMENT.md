# Development

M3 is a large, active codebase. This guide is intended for all potential
contributors, designed to make it easy to get started with contributing to M3.

## Setup your development environment

First, fork https://github.com/m3db/m3 to your own user or org account on GitHub.

Then, clone your fork:

```bash
mkdir -p $GOPATH/src/github.com/m3db
cd $GOPATH/src/github.com/m3db
git clone git@github.com:<fork_user_or_org>/m3.git
```

Note: All commits/branches/etc must be pushed to the fork, not to m3db/m3.

Verify that `git clone` properly set the repository's upstream:

```
# git remote -v
origin  git@github.com:<fork_user_or_org>/m3.git (fetch)
origin  git@github.com:<fork_user_or_org>/m3.git (push)
```

Install dependencies:

```bash
cd m3/
make install-vendor-m3
```

If everything is set up correctly, `m3dbnode` should build successfully:

```bash
make m3dbnode
```

## Running the M3 stack locally

Follow the instructions in [this README][local-readme].

[local-readme]: ./scripts/development/m3_stack/README.md

## Updating Mocks And Generated Files

If changes require updates to mocks or other generated files, make sure to
update those files. There are `make` targets to help with generation:

- Mocks: `make mock-gen`
- Protobuf: `make proto-gen`
- Thrift: `make thrift-gen`

Don't forget to account for changes to generated files in tests!

## Testing Changes

M3 has an extensive test suite to ensure that we are able to validate changes.
More notes about the various testing strategies we employ can be found in
[TESTING][TESTING.md].

While our CI suite will run all tests prior to allowing pull requests to be
merged, developers should test their changes during development and prior to
pushing updates. To test code, use `go test`:

```
go test -v ./src/...
```

Unfortunately, as more tests have been added, the time required to run the
entire suite has increased significantly; therefore, we recommend at least
running tests for both (1) any packages with changes and (2) any packages that
import the changed packages. An example of this would be:

```bash
changed=$(git diff --name-only HEAD^ HEAD | xargs -I {} dirname {} | sort | uniq)
for pkg in $changed; do
  affected=$(grep -r "$pkg" ./src | cut -d: -f1 | grep -v mock | xargs -I{} dirname {} | sort | uniq)
  go test -v -race "./$pkg" $affected
done
```

Contributors are free to do whatever due diligence that they feel helps them to
be most productive. Our only request is that CI jobs are not used as a
first-pass filter to determine whether a change is sane or not.

Once tests are passing locally, push to a new remote branch to create a pull
request, or push to an existing branch to update a pull request. If the CI
suite reports any errors, attempt to reproduce failures locally and fix them
before continuing.

For larger or more intensive tests (e.g. "big" unit tests, integration tests),
additional build tags may be necessary to scope the tests down to a smaller
subset, e.g.:

```
# example integration test
$ go test ./integration -tags integration -run TestIndexBlockRotation -v

# example big unit test
$ go test -tags big ./services/m3dbnode/main -run TestIndexEnabledServer -v
```

## Code Review

Please observe the following guidelines when submitting or reviewing pull
requests:

- Merging is blocked on approval by 1 or more users with write access. We
  recommend 2+ for large, complex, or nuanced changes.
- Pull requests should contain clear descriptions and context per the
  pull request template.
- Breaking changes under the current versioning guarantees (see
  [COMPATIBILITY][COMPATIBILITY.md]) must be approved by the
  [Technical Steering Committee (TSC)][GOVERNANCE.md].
- The [STYLEGUIDE][STYLEGUIDE.md] should be followed to the extent reasonable
  within the scope of the pull request.
- Changed codepaths should be validated by unit tests that cover at least one
  nominal case and one error case.
- Pull requests may only be merged with a green build. Do not merge with
  build failures, even if they are known and unrelated.
- Flaky or otherwise unreliable CI failures count as hard failures. Commit fixes
  or skips for flaky tests or other failures first!

### Scoping pull requests

Significantly inspired by Phabricator's article about
[Recommendations on Revision Control][phab-one-idea], and particularly
because pull requests tend to be squash-merged, try to keep PRs focused
on one idea (or the minimal number of ideas for the change to be viable).

Some of the advantages of smaller PRs are:
- quicker to write + quicker to review = faster iteration
- easier to spot errors
- less cognitive overhead (and net time invested) for reviewers due to
  reduced scope
- naturally avoids scope creep
- clearly establishes that all parts of the PR are related

Because of this, contributors are encouraged to keep PRs as small as
possible. Given that this does introduce some developer overhead (e.g.
needing to manage more PRs), *how* small is unspecified; reviewers
can request PRs to be broken down further as necessary, and contributors
should work with reviewers to find the right balance.

[phab-one-idea]: https://secure.phabricator.com/book/phabflavor/article/recommendations_on_revision_control/#one-idea-is-one-commit

## Updating the CHANGELOG

After a pull request is merged, significant changes must be summarized in the
[CHANGELOG][CHANGELOG.md]. Since the PR number is not known ahead of time, this
must be done as a subsquent commit after the merged PR (though the CHANGELOG
PR can be prepared in tandem with the referenced PR).

The format of the CHANGELOG entry should be:

```
- TYPE **COMPONENT**: Description (#PR_NUMBER)
```

The `TYPE` should be ommitted if it is a feature. If the change is not a
feature, `TYPE` should be either `FIX` or `PERF`. New types of changes should
be added to this document before used if they cannot be categorized by the
existing types.

New CHANGELOG entries should be added to the current "unreleased" section at
the top of the CHANGELOG as long as they are within the scope of that potential
version. If no unreleased section exists, one should be added using the
appropriate proposed semver. If an unreleased section exists but the new change
requires a different semver change (e.g. the unreleased version is a patch
version bump, but the new change requires a minor version bump), the version
on the existing section should be updated. See [COMPATIBILITY][COMPATIBILITY.md]
for more information on versioning.

An example CHANGELOG addition:

```
# 0.4.5 (unreleased)

- FIX **DB**: Index data race in FST Segment reads (#938)
```

## Building Docs

Documentation is located adjacent to other [m3db.io][m3db.io] assets in
[site/content/docs/][docs-path]. These markdown files are built into a static
site using Hugo and Docker through `make docs-build`.

Documentation changes can be tested locally by simply running a Hugo webserver
(installing Hugo is required for this step: simply `brew install hugo` or see
Hugo's [Quick Start][hugo-quick-start] for more options). Simply run:

```
cd site
hugo server
```

once Hugo is installed to run a local webserver that will live-update as
changes are made to the documentation or rest of the site.

## M3 Website

The M3 website, [m3db.io][m3db.io], is hosted with Netlify. It is configured to
run `make site-build` and then serve the contents of the `site/public/`
directory. The site is built and republished on every commit to master.

## Cutting a release

1. Ensure you have a GitHub API access token with the `repo` scope
2. Checkout the commit you want to release (all releases must be on master)
3. Create a tag with the version you would like to release
   - E.g. `git tag -a v0.7.0 -m "v0.7.0"`
   - See [COMPATIBILITY][COMPATIBILITY.md] for semver information.
4. Push the tag
   - E.g. `git push origin v0.7.0`
5. Run `make GITHUB_TOKEN=<GITHUB_API_ACCESS_TOKEN> release`
6. Update `CHANGELOG.md` and commit it to master
7. Copy and paste the text from `CHANGELOG.md` into the release notes [on GitHub][releases].

[CHANGELOG.md]: https://github.com/m3db/m3/blob/master/CHANGELOG.md
[COMPATIBILITY.md]: https://github.com/m3db/m3/blob/master/COMPATIBILITY.md
[GOVERNANCE.md]: https://github.com/m3db/m3/blob/master/GOVERNANCE.md
[STYLEGUIDE.md]: https://github.com/m3db/m3/blob/master/STYLEGUIDE.md
[TESTING.md]: https://github.com/m3db/m3/blob/master/TESTING.md
[docs-path]: https://github.com/m3db/m3/tree/master/site/content/docs
[hugo-quick-start]: https://gohugo.io/getting-started/quick-start/
[m3db.io]: https://m3db.io/
[releases]: https://github.com/m3db/m3/releases
