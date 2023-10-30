#!/bin/bash
set -eux

bazelisk run //docker/konfig:docker_compose -- down