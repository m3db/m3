# uber-licence

<!--
    [![build status][build-png]][build]
    [![Coverage Status][cover-png]][cover]
    [![Davis Dependency status][dep-png]][dep]
-->

<!-- [![NPM][npm-png]][npm] -->

<!-- [![browser support][test-png]][test] -->

Utility to deal with Uber OSS licences

## Example

`uber-licence`

Running the `uber-licence` binary adds licencing information
  to every javascript file in your project.

You can run `uber-licence --dry` where it does not
  mutate any files and instead outputs -1.

You can use `--file` and `--dir` to specify your own file 
  and directory filters to select source files to consider.

## Recommended usage

```js
// package.json
{
  "scripts": {
    "check-licence": "uber-licence --dry",
    "add-licence": "uber-licence"
  },
  "devDependencies": {
    "uber-licence": "uber/uber-licence",
    "pre-commit": "0.0.9"
  },
  "pre-commit": [
    "test",
    "check-licence"
  ],
  "pre-commit.silent": true
}
```

We recommend you add two scripts to your package and run
  `check-licence` in a git pre commit.

## Installation

`npm install uber-licence`

## Tests

`npm test`

## Contributors

 - Raynos


## MIT Licenced

  [build-png]: https://secure.travis-ci.org/uber/uber-licence.png
  [build]: https://travis-ci.org/uber/uber-licence
  [cover-png]: https://coveralls.io/repos/uber/uber-licence/badge.png
  [cover]: https://coveralls.io/r/uber/uber-licence
  [dep-png]: https://david-dm.org/uber/uber-licence.png
  [dep]: https://david-dm.org/uber/uber-licence
  [test-png]: https://ci.testling.com/uber/uber-licence.png
  [tes]: https://ci.testling.com/uber/uber-licence
  [npm-png]: https://nodei.co/npm/uber-licence.png?stars&downloads
  [npm]: https://nodei.co/npm/uber-licence
