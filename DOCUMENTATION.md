# Documentation Guide

M3 is a large and complex project, and any help you can offer to explain it better is most welcome.

## Setup Documentation Locally

### Prerequisites

The M3 documentation uses [Hugo](https://gohugo.io/), a static site generator written in Go. [Find installation instructions in the Hugo documentation](https://gohugo.io/getting-started/installing/).

If you also want to run and serve the M3 documentation, you need our custom theme, [Victor](https://github.com/chronosphereio/victor). Clone or download it into the same directory as the M3 codebase.

## Folder Structure

Hugo hosts the M3 documentation, website, and Open API spec.

### Website

The website is the HTML, CSS, and JavaScript files in the _site/static_ folder. Edit those files to make any changes.

### Open API docs

Hugo hosts the Open API spec from the _site/static/openapi_ folder. To make changes to the spec, edit the _spec.yml_ file in _src/query/generated/assets_, and a CI job generates the file that Hugo hosts automatically.

### Configuration

The _site/config_ folder has configuration for Hugo, split into three folders: 

-   A default  _config.toml_ file in _site/config/\_default_
-   Overridden configuration for development and production environments in _site/config/development_ and _site/config/production_ respectively.

### Theme Overrides

The _site/layouts_ folder adds a several changes and additional files to the Victor theme used by the M3 documentation.

### Documentation

The _site/content_ folder contains the documentation files, organized by folders that match the paths for URLs. The _includes_ folder is a special folder not served as part of the documentation and contains files used by other files.