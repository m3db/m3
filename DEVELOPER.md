# Developer Notes

## Setup your development environment

First fork https://github.com/m3db/m3 into your own workspace on Github.

Then create your clone:

```bash
export working_dir=$GOPATH/src/github.com/m3db
mkdir -p $working_dir

# Set this to your Github user
export user="your github profile name"

# Clone your fork
cd $working_dir
git clone git@github.com:$user/m3.git
# or: https://github.com/$user/m3.git

# Set upstream
cd m3
git remote add upstream git@github.com:m3db/m3.git
# or: https://github.com/m3db/m3.git

# Never push to upstream master
git remote set-url --push upstream no_push

# Check if it makes sense:
git remote -v
```

Install dependencies:

```bash
cd $working_dir/m3

make install-vendor-m3
```

If everything is setup correctly you should be able to build `m3dbnode`:

```bash
make m3dbnode
```

## Running the M3 stack locally

Follow the instructions in `./scripts/development/m3_stack/README.md`

## Testing Changes

M3 has an extensive, and ever increasing, set of tests to ensure we are able to validate changes. More notes about the various testing strategies we employ can be found in `TESTING.md`. An unfortunate consequence of the number of tests is running the test suite takes too long on a developer's laptop. Here's the workflow most developers employ to be productive. Note: take this as a suggestion of something that works for some people, not as a directive. Do what makes you enjoy the development process most, including disregarding this suggestion!

Once you have identified a change you want to make, and gathered consensus by talking to some devs, go ahead and make a branch with the changes. To test your changes:

(0) If you have updated an interface that has been mocked, you need to update the generated `gomock `files.

```shell
# Generate mocks for all top level packages
make mock-gen

# If you just want to generate it for a single package,
# replace xyz with the package you want to generate files for, e.g. dbnode
make mock-gen-xyz
```

(1) Run unit tests locally
```
go test ./... -v
```

This usually runs within 1minute on most laptops. And catches basic compilation errors quickly.

(2) Submit to CI, and continue to develop further.

(3) Once the CI job finishes - investigate failures (if any), and reproduce locally.

For e.g. if a Unit Tests `TestXYZ` in package `github.com/m3db/m3/xyz` failed, run the following:

```
$ go test ./xyz -run TestXYZ -race -v
```

Similarly, for 'Big Unit Tests'/'Integration' tests, you may have to provide an additional build tag argument, e.g.

```
# example integration test
$ go test ./integration -tags integration -run TestIndexBlockRotation -v

# example big unit test
$ go test  -tags big ./services/m3dbnode/main -run TestIndexEnabledServer -v
```

(4) Fix, rinse, and repeat.

(5) Polish up the PR, ensure CI signs off, and coverage increases. Then ping someone to take a look and get feedback!

## Adding a Changelog

When you open a PR, if you have made are making a significant change you must also update the contents of `CHANGELOG.md` with a description of the changes and the PR number.  You will not know the PR number ahead of time so this must be added as a subsequent commit to an open PR.

The format of the change should be:
```
- TYPE **COMPONENT:** Description (#PR_NUMBER)
```

The `TYPE` should be ommitted if it is a feature.  If it is not a feature it should be one of: `FIX`, `PERF`.  New types of changes should be added to this document before used if they cannot be categorized by the existing types.

Here is an example of an additiong to the pending changelog:

```
# 0.4.5 (unreleased)

- FIX **DB** Index data race in FST Segment reads (#938)
```

## Building the Docs

The `docs` folder contains our documentation in Markdown files. These Markdown files are built into a static site using
[`mkdocs`](https://www.mkdocs.org/) with the [`mkdocs-material`](https://squidfunk.github.io/mkdocs-material/) theme.
Building the docs using our predefined `make` targets requires a working Docker installation:

```
# generate the docs in the `site/` directory
make docs-build

# build docs and serve on localhost:8000 (with live reload)
make docs-serve

# build the docs and auto-push to the `gh-pages` branch
make docs-deploy
```

## M3 Website

The [M3 website](https://m3db.io/) is hosted with Netlify. It is configured to run `make site-build` and then serving the contents of the `/m3db.io` directory. The site is built and republished every time
there is a push to master.

## Cutting a release

1. Ensure you have a Github API access token with the `repo` scope
2. Checkout the commit you want to release (do not release commits that don't exist on master)
3. Create a tag with the version you would like to release. Ex: `git tag -a v0.7.0 -m "v0.7.0"`
4. Push the tag. Ex: `git push origin v0.7.0`
5. Run `make GITHUB_TOKEN=<GITHUB_API_ACCESS_TOKEN> release`
6. Update `CHANGELOG.md` and commit it to master
7. Copy and paste the text from `CHANGELOG.md` into the release notes [on Github](https://github.com/m3db/m3/releases)
