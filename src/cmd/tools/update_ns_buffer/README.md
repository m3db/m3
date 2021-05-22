# `update_ns_buffer`

`update_ns_buffer` allows updating `bufferPast` and `bufferFuture` for a namespace.

## Usage

1. Build the tool:
```
$ make update_ns_buffer
mkdir -p /home/matt/go/src/github.com/m3db/m3/bin
Building update_ns_buffer
go build -o /home/matt/go/src/github.com/m3db/m3/bin/update_ns_buffer ./src/cmd/tools/update_ns_buffer/main/.

$ ./bin/update_ns_buffer -h
Usage of ./bin/update_ns_buffer:
  -buffer-future duration
        new buffer future value (will not be updated if 0)
  -buffer-past duration
        new buffer past value (will not be updated if 0)
  -etcd-addr string
        address of etcd server (default "127.0.0.1:2379")
  -m3-env string
        name of environment to update
  -namespace string
        namespace to update
```

2. Connect to an etcd machine. If running inside of Kubernetes, you can `port-forward`:

```
kubectl port-forward etcd-0 2379
```

3. Specify the environment name you have set in your M3DB config (`env: FOO`) and namespace you wish to update. The tool
   will inform you of the current namespace options. If you do not specify `-buffer-past` or `-buffer-future` the update
   will be a noop.

```
$ ./bin/update_ns_buffer -namespace metrics-10s:2d -m3-env "m3/m3db-cluster" -buffer-past 12m -buffer-future 15m
...
2021-05-22T16:56:16.847Z        INFO    main/main.go:93 current buffers {"bufferPast": "15m0s", "bufferFuture": "10m0s"}
2021-05-22T16:56:16.847Z        INFO    main/main.go:99 setting new value for buffer future     {"duration": "15m0s", "nanos": 900000000000}
2021-05-22T16:56:16.848Z        INFO    main/main.go:107        setting new value for buffer past       {"duration": "12m0s", "nanos": 720000000000}
2021-05-22T16:56:16.848Z        INFO    main/main.go:114        press Enter to continue. ctrl-c to cancel
```

Press `Enter` to confirm the settings and update etcd. To abort, enter `ctrl-c`.

```
2021-05-22T16:56:16.848Z        INFO    main/main.go:114        press Enter to continue. ctrl-c to cancel

2021-05-22T16:57:11.444Z        INFO    main/main.go:126        successfully updated namespace  {"version": 5}
```

You can run the tool again and then abort to confirm the settings are as you expect:
```
$ ./bin/update_ns_buffer -namespace metrics-10s:2d -m3-env "m3/m3db-cluster" -buffer-past 12m -buffer-future 15m
...
2021-05-22T16:57:40.705Z        INFO    main/main.go:93 current buffers {"bufferPast": "12m0s", "bufferFuture": "15m0s"}
...
2021-05-22T16:57:40.705Z        INFO    main/main.go:114        press Enter to continue. ctrl-c to cancel
^C
```
