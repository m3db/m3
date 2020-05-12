m3em
==============================================================================================

`m3em` (pronounced `meme`) is an acronym for M3 Environment Manager. [ccm](https://github.com/pcmanus/ccm)`:C* :: m3em:m3db`. Unlike `ccm`, `m3em` permits remote host operations.

The goal of `m3em` is to make it easy to create, manage and destroy services across hosts. It is meant for testing clustered services like [m3db](https://github.com/m3db/m3) and [m3aggregator](https://github.com/m3db/m3) .

## Components
There are two primary components in m3em:

(1) API constructs encapsulating placement interactions (see `cluster` package), along with remote process orchestration (see `node` package).

(2) `m3em_agent`: process running on remote hosts. It's responsible for process lifecycle, heartbeating back to the coordinating host.

## Usage Example
- For API usage, refer `tools/dtest` in [M3DB](https://github.com/m3db/m3)

### m3em_agent

```
$ make m3em_agent
$ scp ./out/m3em_agent <remote-host>:<remote-path>
$ ssh <remote-host>
$ cat >m3em.agent.yaml <<EOF
server:
  listenAddress: "0.0.0.0:14541"
  debugAddress: "0.0.0.0:24541"

metrics:
  sampleRate: 0.02
  m3:
    hostPort: "127.0.0.1:9052"
    service: "m3em"
    includeHost: true
    env: "development"

agent:
  workingDir: /var/m3em-agent
  startupCmds:
    - path: /bin/echo
      args:
        - "sample startup command"
  releaseCmds:
    - path: /bin/echo
      args:
        - "sample release command"
  testEnvVars:
    UBER_DATACENTER: sjc1
EOF
$ /remote-path/m3em_agent -f m3em.agent.yaml
```
