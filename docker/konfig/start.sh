#!/bin/bash
set -eux

echo "Building konfig proxy"
bazelisk run //go/konfig/src/main/proxy:konfig_proxy

echo "Building konfig server"
bazelisk run //go/konfig/src/main/app:konfig

echo "Building konfig server"
bazelisk run //docker/mongodb:6

# spinning up docker cluster
bazelisk run --sandbox_debug //docker/konfig:docker_compose -- up

#until curl -s -f -o /dev/null curl -X GET "localhost:8080/health"
#do
#  echo "Waiting for konfig server"
#  sleep 5
#done