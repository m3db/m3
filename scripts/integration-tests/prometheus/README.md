# Prometheus and M3DB integration 

This example will demostrate how to run M3DB and Prometehus within the same
host using Docker Compose to bootstrap individual containers.  This guide 
assumes that the code base for M3DB is the locally checkout copy of the 
[repository](https://github.com/m3db/m3https://github.com/m3db/m3) as 
the local repository will be used for the local build of the cluster. 

### Build and run the stack 

First build the images needed for the containers. 

1.) `docker-compose -f docker-compose.yml build`

The first container to be created will be the M3DB node. 

2.) `docker-compose -f docker-compose.yml up dbnode01`

Once the node is up and running follow the [HOWTO](https://github.com/m3db/m3/blob/master/docs/how_to/single_node.md) to configure the M3DB node. 

3.) `docker-compose -f docker-compose.yml up prometheus01`
