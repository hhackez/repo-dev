#!/bin/bash -l
set -e

CUR_PATH="$(cd "$(dirname "$0")" && pwd)"
export JOB_HOME=${JOB_HOME:-${CUR_PATH}}
echo ">> JOB_HOME: ${JOB_HOME}"

ENV=${ENV:-dev}

SPARK_SUBMIT_CONF=(
--conf spark.executor.cores=1
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=128mb
--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=256mb
--conf spark.sql.files.maxPartitionBytes=256mb
--conf spark.sql.files.maxRecordsPerFile=1000000
--conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MILLIS
)

# Convert the array to a string, with elements separated by a special delimiter
SPARK_SUBMIT_CONF_STR=$(IFS=$'\x1E'; echo "${SPARK_SUBMIT_CONF[*]}")

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

# execute
case ${TASK_NAME} in
    create) 
        bash ${JOB_HOME}/pyspark-submit.sh \
            ${TASK_NAME} 3g 6g '' ${SPARK_SUBMIT_CONF_STR} \
            src/main.py \
            -c $CONFIG_FILE \
            -t $TASK_NAME \
            -y $YYYYMMDD
        ;;
    upload)
        bash ${JOB_HOME}/pyspark-submit.sh \
            ${TASK_NAME} 3g 6g '' ${SPARK_SUBMIT_CONF_STR} \
            src/main.py \
            -c $CONFIG_FILE \
            -t $TASK_NAME \
            -y $YYYYMMDD
        ;;
    merge)
        bash ${JOB_HOME}/pyspark-submit.sh \
            ${TASK_NAME} 3g 6g '' ${SPARK_SUBMIT_CONF_STR} \
            src/main.py \
            -c $CONFIG_FILE \
            -t $TASK_NAME \
            -y $YYYYMMDD
        ;;
    # serve)
    #     python src/serve.py \
    #         --c $CONFIG_FILE \
    #         --t ${TASK_NAME} \
    #     ;;
    # testcase)
    #     bash ${JOB_HOME}/pyspark-submit.sh \
    #         ${TASK_NAME} 1g 2g '' ${SPARK_SUBMIT_CONF_STR} \
    #         src/main.py \
    #         -c $CONFIG_FILE \
    #         -t $TASK_NAME \
    #         -y $YYYYMMDD
    #     ;;
    *)
        echo ">> Wrong task_name \`${TASK_NAME}\`."
        print_usage
        exit 1
        ;;
esac
