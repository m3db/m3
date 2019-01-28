Kernel Configuration
====================

This document lists the Kernel tweaks M3DB needs to run well.

## vm.max_map_count
M3DB uses a lot of mmap-ed files for performance, as a result, you might need to bump `vm.max_map_count`. We suggest setting this value to `3000000`, so you don’t have to come back and debug issues later.

On Linux, you can increase the limits by running the following command as root:
```
sysctl -w vm.max_map_count=3000000
```

To set this value permanently, update the `vm.max_map_count` setting in `/etc/sysctl.conf`.

## vm.swappiness
`vm.swappiness` controls how much the virtual memory subsystem will try to swap to disk. By default, the kernel configures this value to `60`, and will try to swap out items in memory even when there is plenty of RAM available to the system.

We recommend sizing clusters such that M3DB is running on a substrate (hosts/containers) such that no-swapping is necessary, i.e. the process is only using 30-50% of the maximum available memory. And therefore recommend setting the value of `vm.swappiness` to `1`. This tells the kernel to swap as little as possible, without altogether disabling swapping.

On Linux, you can configure this by running the following as root:
```
sysctl -w vm.swappiness=1
```

To set this value permanently, update the `vm.swappiness` setting in `/etc/sysctl.conf`.


## rlimits
M3DB also can use a high number of files and we suggest setting a high max open number of files due to per partition fileset volumes.

On Linux you can set a high limit for maximum number of open files in `/etc/security/limits.conf`:
```
your_m3db_user        hard nofile 500000
your_m3db_user        soft nofile 500000
```

Alternatively, if you wish to have M3DB run under `systemd` you can use our [service example](https://github.com/m3db/m3/tree/master/integrations/systemd/m3dbnode.service) which will set sane defaults.

Before running the process make sure the limits are set, if running manually you can raise the limit for the current user with `ulimit -n 500000`.