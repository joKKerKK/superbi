#!/bin/bash -e

DEBUG_MODE=OFF

####### PACKAGE PROPERTIES ################
PACKAGE=__PACKAGE__
USER=${PACKAGE}
GROUP=fdp
USER_ID=914299      #whats user id?

######## COMMON_DIR ####################
BASE_DIR=/usr/share/${PACKAGE}
LOG_DIR=/var/log/flipkart/${PACKAGE}
CRON_DIR=/etc/cron.d/${PACKAGE}
WORK_DIR=/var/lib/flipkart/${PACKAGE}
SERVICE_DIR=/etc/service/${PACKAGE}
INIT_FILE=/etc/init.d/${PACKAGE}

SERVER_DIR=${BASE_DIR}/server
SERVER_LOG_DIR=${LOG_DIR}/server
SERVER_SCRIPTS_DIR=${SERVER_DIR}/scripts
SERVER_WORK_DIR=${WORK_DIR}/server
SERVICE_WRAPPER_FILE=${SERVER_SCRIPTS_DIR}/start.sh

##### LOGGING #########################
SERVER_LOG_FILE=/var/log/flipkart/${PACKAGE}/server/server.log
GC_LOG=${LOG_DIR}/gc/gc.log

DW_YAML_FILE=${WORK_DIR}/server.yaml

##### BUCKET PROPERTIES ###############
BUCKET_FILE=/etc/default/${PACKAGE};
BUCKET_FILE_OVERRIDE=/etc/default/${PACKAGE}_override;
#This property has to be set in config service as well
BACKUP_BUCKET_FILE=${SERVER_WORK_DIR}/${PACKAGE}-config.json


##### SERVER STARTUP PROPERTIES #######
TARGET_JAR=${SERVER_DIR}/lib/${PACKAGE}.jar
APP_PROPS="-Dcom.sun.management.jmxremote.port=21214 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=false"
GC_PROPS="-XX:+PrintGCDateStamps -verbose:gc -XX:+PrintGCDetails -Xloggc:${GC_LOG} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M"
HEAP_DUMP_PROPS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/var/log/flipkart/${PACKAGE}/heap/heap-`date +"%T"`.hprof"
MEMORY_OPTS="${MEMORY_OPTS}"
GC_TYPE_PROPS="-XX:+UseG1GC -XX:+PrintTenuringDistribution ${GC_TYPE_PROPS}"


##### HEALTH CHECK PROPERTIES ##########
##### TODO: Remove
HEALTH_CHECK_FLAG_FILE=${SERVER_WORK_DIR}/remove-this-file-to-bbr
HEALTH_CHECK_URL="http://localhost:21212/health"
HEALTH_CHECK_NAME="${PACKAGE} Server"
HEALTH_CHECK_WRAPPER=${SERVER_SCRIPTS_DIR}/health-check.sh
POLLER_FILE=${SERVER_SCRIPTS_DIR}/poll.sh


if [ "${DEBUG_MODE}" == "ON" ]; then
    set -x
elif [ "${DEBUG_MODE}" == "OFF" ]; then
    set +x
else
    set +x
fi

function LOG {
	log_level=$1
	message=$2
	echo [`date +"%Y-%m-%d %H:%M:%S"`] [${log_level}] : ${message}
}