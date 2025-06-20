#!/bin/bash

set -ex

BUILD_TAG=$1

if [ -z "${BUILD_TAG}" ]; then
  echo "Usage: $0 <build-tag>.   E.g. build_and_push_docker.sh 0.1.0"
  exit 1
fi

docker buildx build --platform=linux/amd64 \
  --cache-from type=local,src=/tmp/cache/amd64 \
  --cache-to type=local,dest=/tmp/cache/amd64 \
  --ssh default \
  --tag registry.ddbuild.io/dfray-amd64:"${BUILD_TAG}" \
  -f Dockerfile.distributed-datafusion .

docker push registry.ddbuild.io/dfray-amd64:"${BUILD_TAG}"

docker buildx build --platform=linux/arm64 \
  --cache-from type=local,src=/tmp/cache/arm64 \
  --cache-to type=local,dest=/tmp/cache/arm64 \
  --ssh default \
  --tag registry.ddbuild.io/dfray-arm64:"${BUILD_TAG}" \
  -f Dockerfile.distributed-datafusion .

docker push registry.ddbuild.io/dfray-arm64:"${BUILD_TAG}"

# unsure why this does not work at the moment, for now push each build
# docker manifest create --amend \
#   registry.ddbuild.io/dfray:"${BUILD_TAG}" \
#   registry.ddbuild.io/dfray-amd64:"${BUILD_TAG}" \
#   registry.ddbuild.io/dfray-arm64:"${BUILD_TAG}"
#
# docker manifest annotate registry.ddbuild.io/dfray:"${BUILD_TAG}" registry.ddbuild.io/dfray-amd64:"${BUILD_TAG}" --os linux --arch amd64
# docker manifest annotate registry.ddbuild.io/dfray:"${BUILD_TAG}" registry.ddbuild.io/dfray-arm64:"${BUILD_TAG}" --os linux --arch arm64

# docker manifest push registry.ddbuild.io/dfray:"${BUILD_TAG}"
