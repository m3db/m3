# M3DB on Kubernetes

This doc is aimed at developers building M3DB on Kubernetes. End users should see our
[how-to](https://docs.m3db.io/how_to/kubernetes) guide for more info.

## Bundling

In order to make it possible to set up m3db using a single YAML file that can be passed as a URL to `kubectl apply`, we
bundle our manifests into a single `bundle.yaml` file. In order to create a bundle, simply run `build_bundle.sh`. It
will take care of ordering (i.e. `Namespace` object comes first) and produce a single YAML file.
