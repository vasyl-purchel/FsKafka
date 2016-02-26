#!/bin/bash

# stop all running images
docker stop $(docker ps -aq)

# remove them
docker rm $(docker ps -aq)
