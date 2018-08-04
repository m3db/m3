# Developer Notes
=================

## Testing Changes
M3DB has an extensive (and ever increasing) set of tests to ensure we are able to validate changes. More notes about the various testing strategies we employ can be found in `TESTING.md`. An unfortunate consequence of the number of tests is running the test suite takes too long on a developer's laptop. Here's the workflow most developers employ to be productive. Note: take this as a suggestion of something that works for some people, not as a directive. Do what makes you enjoy the development process most, including disregarding this suggestion!

Once you have identified a change you want to make, and gathered consensus by talking to some devs, go ahead and make a branch with the changes. To test your changes:

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

## M3DB Website
The [M3DB website](https://m3metrics.io/) is hosted via netlify. It is configured to run `make site-build` and then serving the contents of the /m3metrics.io directory. The site is built and republished every time
there is a push to master.
