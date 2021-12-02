# Documentation Guide

M3 is a large and complex project, and any help you can offer to explain it better is most welcome. If you have a suggestion for the documentation M3 welcomes it, and documentation pull requests follow the same process as [code contributions](CONTRIBUTING.md).

The rest of this document explains how to setup the documentation locally and the structure of the content.

## Setup Documentation Locally

### Prerequisites

The M3 documentation uses [Hugo](https://gohugo.io/), a static site generator written in Go. [Find installation instructions in the Hugo documentation](https://gohugo.io/getting-started/installing/).

If you also want to run and serve the M3 documentation, you need the custom theme, [Victor](https://github.com/chronosphereio/victor). Clone or download it into the same directory as the M3 codebase.

To run some tests and test the production build steps (this doesn't run M3 itself), you need [Docker](https://www.docker.com).

## Folder Structure

Hugo hosts the M3 documentation, website, and Open API spec.

### Website

The website is the HTML, CSS, and JavaScript files in the _site/static_ folder. Edit those files to make any changes.

### Open API Docs

Hugo hosts the Open API spec from the _site/static/openapi_ folder. To make changes to the spec, edit the _spec.yml_ file in _src/query/generated/assets_, and a CI job generates the file that Hugo hosts automatically.

### Configuration

The _site/config_ folder has configuration for Hugo, split into three folders:

-   A default  _config.toml_ file in _site/config/\_default_
-   Overridden configuration for development and production environments in _site/config/development_ and _site/config/production_ respectively.

### Theme Overrides

The _site/layouts_ folder adds a several changes and overridden files to the Victor theme used by the M3 documentation.

### Documentation

The _site/content_ folder contains the documentation files, organized by folders that match the paths for URLs. The _includes_ folder is a special folder not served as part of the documentation and files used by other files.

#### Theme

The M3 documentation uses its own (open source) theme, Victor. You can read all the features that theme provides in [the repository for Victor](https://github.com/chronosphereio/victor).

Victor is a theme based on Hugo modules, [read more in the Hugo docs](https://gohugo.io/hugo-modules/use-modules/) about how to use and update it.

#### Shortcodes

The M3 documentation adds the following extra shortcodes:

-   `{{% apiendpoint %}}` - Combines the values of `Params.api.localCordinator` + `Params.api.apiEndpoint` as defined in the site configuration to output the base API endpoint for M3 running on a localhost.
-   `{{% docker-version %}}` - Outputs the value of `Params.releases.docker` as defined in the site configuration to output the consistent current Docker release.
-   `{{% now %}}` - Outputs the current Unix timestamp to allow for up to date timestamps in code examples.

## Running Documentation

As noted in the prerequisites section, if you want to run the documentation locally to see how your edits look with `hugo server`, you need to have the Victor theme in the same parent directory as the M3 codebase, as `hugo server` runs in Hugo's "development" mode (and matches _site/config/development/config.toml_).

This does mean that as you make changes to the theme or documentation, it refreshes automatically in the browser preview. Sometimes Hugo doesn't refresh included files, so you may need to restart the server process.

## Testing Documentation

The M3 documentation has a number of tests that need to pass before a pull request is merged to master and deployed. These include:

-   Link checking with htmltest

To run them you need Docker installed and running, and at the top level of the M3 project, run `make clean install-vendor-m3 docs-test`. If the test is successful you see green text, if not, htmltest tells you which links in which pages you need to fix.

## Building Documentation

There are a couple of different ways to build the documentation depending what you want to do.

- If you want to build only the most up-to-date version of the docs, you can use `hugo` to build.
- If you want to build all versions of the docs using Hugo run in Docker (this is what CI does to test the documentation). From the top level of the project, run `make docs-build`.
- If you want to build all versions of the docs with a system-installed version of Hugo (this is what Netlify does to build and serve the documentation). From the top level of the project, run `make site-build`.

## Creating a New Documentation Version

M3 releases versions with some slight changes to documentation for each one which users can access from the drop down menu under the left hand navigation.

Archiving a version of the documentation is a slightly complex process as Hugo doesn't natively support versioning and the documentation uses Hugo modules to accomplish this.

1. Add the new version to the _config/production/config.toml_ file in the `[[module.imports.mounts]]` section.

    ```toml
    [[module.imports.mounts]]
    source = "content/v{VERSION_NUMBER}"
    target = "content/docs/v{VERSION_NUMBER}"
    ```

2. Archive the current latest version of the documentation (which now becomes the new version) to the https://github.com/m3db/docs-archive repository, into a sub-folder of _content_ that matches the new version.