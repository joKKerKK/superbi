#!/bin/bash -e
set -e
ulimit -n 100000

PACKAGE=__PACKAGE__

exec 2>&1

source /usr/share/${PACKAGE}/server/commons.sh

# By default infra is kaas
INFRA=${INFRA:=kaas}
if [ "$INFRA" == kaas ]
then
  [ -z ${CONFIG_BUCKET} ] && ( [ -z ${APP_CONFIGMAP} ] ||  [ -z ${POD_NAMESPACE} ] ) && LOG ERROR "Either CONFIG_BUCKET or (APP_CONFIGMAP and POD_NAMESPACE) must be set" && exit 1
elif [ "$INFRA" == iaas ]
then
  # Config Bucket must be provided
  [ -z ${CONFIG_BUCKET} ] && LOG ERROR "CONFIG_BUCKET has to be set" && exit 1
else
   LOG ERROR "INFRA must be either iaas or kaas" && exit 1
fi

[ ! -d "$LOG_DIR" ] && mkdir -p "$LOG_DIR"
chown -Rf $FK_USER:$FK_GROUP "$LOG_DIR" || true

mkdir -p "${LOG_DIR}/gc"
mkdir -p "${LOG_DIR}/heap"
mkdir -p "${LOG_DIR}/request"
mkdir -p "${LOG_DIR}/server"


# Preparing startup logfile
LOG INFO "Staring the server"

# Ensure target jar is set
[ -z ${TARGET_JAR} ] && LOG ERROR "TARGET_JAR has to be set" && exit 1

SVC_CMD="java -Dconfig.svc.buckets=${CONFIG_BUCKET} -DCFG_SVC_BUCKET=${CONFIG_BUCKET} -DENV=${ENV} ${APP_PROPS} ${GC_PROPS} ${GC_TYPE_PROPS} ${HEAP_DUMP_PROPS} ${MEMORY_OPTS} -jar ${TARGET_JAR} server ${DW_YAML_FILE}"

LOG INFO "${SVC_CMD}"
exec $SVC_CMD