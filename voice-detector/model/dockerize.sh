#!/bin/bash -l

set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
echo ">> DIR: ${DIR}"

DOCKERFILE=$DIR/Dockerfile

REPO_URL=${REPO_URL:-803135791119.dkr.ecr.ap-northeast-2.amazonaws.com}
APPNAME=${APPNAME:-reco/voice-detector}
APPTAG=${APPTAG:-voice-detector}

IMGNAME="$REPO_URL/$APPNAME:$APPTAG"

echo "--- Docker build arguments ---"
echo "    DOCKERFILE: $DOCKERFILE"
echo "    IMGNAME: $IMGNAME"
echo "------------------------------"

docker build \
    --tag "${IMGNAME}" \
    -f "$DOCKERFILE" \
    --force-rm --no-cache \
    "$DIR"