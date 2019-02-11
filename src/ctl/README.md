# M3Ctl

Configuration controller for the M3DB ecosystem. Provides an http API to perform CRUD operatations on the various configs for M3DB compontents.

### Run the web service and application

```bash
make m3ctl
./bin/m3ctl -f src/ctl/config/m3ctl.yml
open http://localhost:9000
```

**UI**
`/`

**API Server**
`/r2/v1`

**API Docs (via Swagger)**
`public/r2/v1/swagger`
