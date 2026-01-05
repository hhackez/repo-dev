#!/bin/bash
set -e
DIR="$( cd "$( dirname "$0" )" && pwd )"
cd $DIR
echo ">> DIR: ${DIR}"

ENV=${ENV:-dev}

DOCKERFILE=$DIR/Dockerfile

REPO_URL=803135791119.dkr.ecr.ap-northeast-2.amazonaws.com
APPNAME=$ENV/reco/reco-workflow.voice-detector.data
APPTAG=${APPTAG:-latest}

IMGNAME="$REPO_URL/$APPNAME:$APPTAG"

echo "--- Docker build arguments ---"
echo "    DOCKERFILE: $DOCKERFILE"
echo "    IMGNAME: $IMGNAME"
echo "------------------------------"

#docker buildx build --load \
docker build \
    --tag "${IMGNAME}" \
    -f "$DOCKERFILE" \
    --force-rm \
    $DIR