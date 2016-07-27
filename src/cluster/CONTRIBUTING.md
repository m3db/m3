Contributing
============

We'd love your help making M3DB great!

## Getting Started

M3DB uses Go vendoring to manage dependencies.
To get started:

```bash
git submodule update --init --recursive
make test
```

## Making A Change

*Before making any significant changes, please [open an
issue](https://github.com/m3db/m3db/issues).* Discussing your proposed
changes ahead of time will make the contribution process smooth for everyone.

Once we've discussed your changes and you've got your code ready, make sure
that tests are passing (`make test` or `make cover`) and open your PR! Your
pull request is most likely to be accepted if it:

* Includes tests for new functionality.
* Follows the guidelines in [Effective
  Go](https://golang.org/doc/effective_go.html) and the [Go team's common code
  review comments](https://github.com/golang/go/wiki/CodeReviewComments).
* Has a [good commit
  message](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).
 