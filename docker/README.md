# M3 Docker Builds

M3 docker images are built according to the following policy:

1. For all images, `${IMAGE}:master` will point to the latest build on `origin/master` for that image.

2. For all images, `${IMAGE}:latest` will point to the latest tagged release.

3. For all images, and for all releases, there will be an image tagged `${IMAGE}:${RELEASE}`.

## Builds

This directory contains the Dockerfiles, configs, and build scripts for building images according to the above policy. `images.json` contains the base repo that will be used to tag images, as well as the config for each image. For example, with the following config:

```json
{
  "image_base": "quay.io/m3db",
  "images": {
    "m3dbnode": {
      "dockerfile": "docker/m3dbnode/Dockerfile",
      "aliases": [
        "m3db"
      ]
    }
  }
}
```

`quay.io/m3db/m3dbnode` will be dual-published under `quay.io/m3db/m3dbnode:latest` and `quay.io/m3db/m3db:latest` for
the latest release.
