# M3DB Docker build processes

## Development building and testing 

The `Dockerfile-dev` file is intented to help bootstrap the development
process by providing a simple Dockerfile to perform the build process within 
a container namespace. Docker's container runtime also provides simple way to
simulate production like environment. 

```
docker build -t m3dbnode:latest -f Dockerfile-dev . 
docker run --name m3dbnode m3dbnode:latest 
```

## Production building and release 

The `Dockerfile-release` file is intented to provide the Docker image used within a 
production environment in which the minimal amount of artifacts are placed
within the container.

```
docker build -t m3dbnode:$(git rev-parse head) -f Dockerfile-release . 
docker run --name m3dbnode m3dbnode:$(git rev-parse head)
```
