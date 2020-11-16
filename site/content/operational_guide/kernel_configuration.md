---
title: "Docker & Kernel Configuration"
weight: 10
---

This document lists the Kernel tweaks M3DB needs to run well. If you are running on Kubernetes, you may use our
`sysctl-setter` [DaemonSet](https://github.com/m3db/m3/blob/master/kube/sysctl-daemonset.yaml) that will set these
values for you. Please read the comment in that manifest to understand the implications of applying it.

## Running with Docker

When running M3DB inside Docker, it is recommended to add the `SYS_RESOURCE` capability to the container (using the
`--cap-add` argument to `docker run`) so that it can raise its file limits:

```shell
docker run --cap-add SYS_RESOURCE quay.io/m3/m3dbnode:latest
```

If M3DB is being run as a non-root user, M3's `setcap` images are required:

```shell
docker run --cap-add SYS_RESOURCE -u 1000:1000 quay.io/m3/m3dbnode:latest-setcap
```

More information on Docker's capability settings can be found [here][docker-caps].

## vm.max_map_count

M3DB uses a lot of mmap-ed files for performance, as a result, you might need to bump `vm.max_map_count`. We suggest setting this value to `3000000`, so you don’t have to come back and debug issues later.

On Linux, you can increase the limits by running the following command as root:

```shell
sysctl -w vm.max_map_count=3000000
```

To set this value permanently, update the `vm.max_map_count` setting in `/etc/sysctl.conf`.

## vm.swappiness

`vm.swappiness` controls how much the virtual memory subsystem will try to swap to disk. By default, the kernel configures this value to `60`, and will try to swap out items in memory even when there is plenty of RAM available to the system.

We recommend sizing clusters such that M3DB is running on a substrate (hosts/containers) such that no-swapping is necessary, i.e. the process is only using 30-50% of the maximum available memory. And therefore recommend setting the value of `vm.swappiness` to `1`. This tells the kernel to swap as little as possible, without altogether disabling swapping.

On Linux, you can configure this by running the following as root:

```shell
sysctl -w vm.swappiness=1
```

To set this value permanently, update the `vm.swappiness` setting in `/etc/sysctl.conf`.

## rlimits

M3DB also can use a high number of files and we suggest setting a high max open number of files due to per partition fileset volumes.

You may need to override the system and process-level limits set by the kernel with the following commands. To check the existing values run:

```shell
sysctl -n fs.file-max
```

and

```shell
sysctl -n fs.nr_open
```

to see the kernel and process limits respectively.
If either of the values are less than three million (our minimum recommended value), then you can update them with the following commands:

```shell
sysctl -w fs.file-max=3000000
```

```shell
sysctl -w fs.nr_open=3000000
```

To set these values permanently, update the `fs.file-max` and `fs.nr_open` settings in `/etc/sysctl.conf`.

Alternatively, if you wish to have M3DB run under `systemd` you can use our [service example](https://github.com/m3db/m3/tree/master/integrations/systemd/m3dbnode.service) which will set sane defaults.
Keep in mind that you'll still need to configure the kernel and process limits because systemd will not allow a process to exceed them and will silently fallback to a default value which could cause M3DB to crash due to hitting the file descriptor limit.
Also note that systemd has a `system.conf` file and a `user.conf` file which may contain limits that the service-specific configuration files cannot override.
Be sure to check that those files aren't configured with values lower than the value you configure at the service level.

Before running the process make sure the limits are set, if running manually you can raise the limit for the current user with `ulimit -n 3000000`.

## Automatic Limit Raising

During startup, M3DB will attempt to raise its open file limit to the current value of `fs.nr_open`. This is a benign
operation; if it fails M3DB, will simply emit a warning.

[docker-caps]: https://docs.docker.com/engine/reference/run/#runtime-privilege-and-linux-capabilities
