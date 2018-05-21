# M3DB Docker build processes

## Development building and testing 

The `Dockerfile-*-dev` files is intented to help bootstrap the development
process by providing simple Dockerfiles to perform the build process within 
a container namespace. Docker's container runtime also provides simple way to
simulate production like environment. 

#### Building the m3dbnode development docker container 
```
docker build -t m3dbnode:latest -f Dockerfile-m3dbnode-dev . 
docker run -p 9000:9000 -p 9001:9001 -p 9002:9002 -p 9003:9003 -p 9004:9004 --name m3dbnode m3dbnode:latest 
```

## Production building and release 

The `Dockerfile-*-release` file is intented to provide the Docker image used within a 
production environment in which the minimal amount of artifacts are placed
within the container.

#### Building the m3dbnode production docker container 
```
docker build -t m3dbnode:$(git rev-parse head) -f Dockerfile-release . 
docker run -p 9000:9000 -p 9001:9001 -p 9002:9002 -p 9003:9003 -p 9004:9004 --name m3dbnode m3dbnode:$(git rev-parse head)
```
#### Building a multinode cluster

Change directory to the root of the repositories directory
```
docker-compose -f docker/compose/cluster/docker-compose.yml  build
docker-compose -f docker/compose/cluster/docker-compose.yml  up 
```
