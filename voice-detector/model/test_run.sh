#!/bin/bash -l
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
echo ">> DIR: ${DIR}"
export ENVTYPE=dev
export REPO_URL=803135791119.dkr.ecr.ap-northeast-2.amazonaws.com
export APPNAME=reco/voice-detector
export APPTAG=voice-detector-${ENVTYPE}
CONTAINER_NAME=voice-detector-test-dev
PORT=8042
OPTIONS="-v $DIR/src:/opt/app/src -v $DIR/res:/opt/app/res"

"$DIR"/dockerize.sh
IMGNAME="$REPO_URL/$APPNAME:$APPTAG"
echo "--- Docker run arguments ---"
echo "    ENVTYPE: $ENVTYPE"
echo "    EXPOSE PORT: $PORT"
echo "    IMGNAME: $IMGNAME"
echo "    CONTAINER_NAME: $CONTAINER_NAME"
echo "----------------------------"

docker run -it $OPTIONS --rm \
    -p $PORT:8001 \
    -e ENVTYPE=$ENVTYPE \
    --name $CONTAINER_NAME $IMGNAME \
    bash