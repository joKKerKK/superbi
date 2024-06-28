#!/bin/bash -e

source /usr/share/fdp-superbi-brv2/server/scripts/commons.sh

# Preparing startup logfile
LOG INFO "" > $STARTUP_LOG_FILE

# Ensure target jar is set
[ -z ${TARGET_JAR} ] && LOG ERROR "TARGET_JAR has to be set" >> ${STARTUP_LOG_FILE} && exit 1

# Waiting for bucket file
while [ true ]; do
    LOG INFO "Sourcing ${BUCKET_FILE_OVERRIDE}" >> ${STARTUP_LOG_FILE}
    source ${BUCKET_FILE_OVERRIDE} || true
    if [ -z "${CONFIG_SERVICE_BUCKET}" ]; then
        LOG INFO "Config bucket not set. --${CONFIG_SERVICE_BUCKET}-- Sleeping for 5 sec" >> ${STARTUP_LOG_FILE}
        sleep 5
    else
        LOG INFO "Acquired config bucket : ${CONFIG_SERVICE_BUCKET}"
        break
    fi
done


PID=`sudo -u${PACKAGE} lsof -i | head -2 | tail -1 | tr -s ' ' | cut -d ' ' -f 2`
if [ "$PID" == "" ]; then
    ulimit -n 100000
    cmd="exec sudo -u$USER java ${APP_PROPS} -Dconfig.svc.buckets=${CONFIG_SERVICE_BUCKET} ${GC_PROPS} ${GC_TYPE_PROPS} ${HEAP_DUMP_PROPS} ${MEMORY_OPTS} -jar ${TARGET_JAR} server ${CONFIG_FILE}"
    LOG INFO "Starting ${PACKAGE} using : $cmd" >> ${STARTUP_LOG_FILE}
    $cmd >> ${STARTUP_LOG_FILE} 2>&1
else
    LOG WARN "Process already running with PID : ${PID}, Kill it first"  >> ${STARTUP_LOG_FILE}
fi

sleep 5
