# WARNING: This is Alpha software and not intended for use until a stable release.

## Running a single node of M3DB within a Docker container

### Prerequisites

Docker v1.9+

### Build Docker image with HEAD of master:

`docker build -t m3dbnode:latest .`

### Build Docker image with specific m3db git comment:

`docker build --build-arg GITSHA=d2642e4b858a108de1100121f068740d367923fa -t m3dbnode:latest .`
