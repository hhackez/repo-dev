#!/bin/bash -l
set -e

CUR_PATH="$(cd "$(dirname "$0")" && pwd)"
export JOB_HOME=${JOB_HOME:-${CUR_PATH}}
echo ">> JOB_HOME: ${JOB_HOME}"

ENV=${ENV:-dev}

# usage
function print_usage {
    echo "입력한 파라미터가 잘못되었습니다."
    echo "스크립트 파일을 확인 후 다시 실행해 주세요."
}

if [[ -z $1 ]]; then
    print_usage
    exit 1
fi

# set arguments
TASK_NAME=$1
YYYYMMDD=$2
CONFIG_FILE=res/config.${TASK_NAME}.${ENV}.yml

cat BUILD_INFO.txt


#sleep infinity

echo ">> TASK_NAME: ${TASK_NAME}"
echo ">> YYYYMMDD: ${YYYYMMDD}"
echo ">> CONFIG_FILE: ${CONFIG_FILE}"

# execute
case ${TASK_NAME} in
    detect)
        python src/main.py \
            -c $CONFIG_FILE \
            -t $TASK_NAME \
            -y $YYYYMMDD
        ;;
    dummy) 
        sleep infinity
        ;;
    *)
        echo ">> Wrong task_name \`${TASK_NAME}\`."
        print_usage
        exit 1
        ;;
esac
