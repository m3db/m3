Contributing
============

We'd love your help making tchannel-go great!

## Getting Started

TChannel uses [godep](https://github.com/tools/godep) to manage dependencies.
To get started:

```bash
go get github.com/uber/tchannel-go
go get github.com/tools/godep
cd $GOPATH/src/github.com/uber/tchannel-go
godep restore
make  # tests should pass
```

## Making A Change

*Before making any significant changes, please [open an
issue](https://github.com/uber/tchannel-go/issues).* Discussing your proposed
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
