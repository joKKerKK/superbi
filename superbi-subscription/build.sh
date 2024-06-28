#!/bin/bash
set -e

CURR_DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

for JPATH in /usr/lib/jvm/jdk-8-oracle-x64; do
    [ -x "$JPATH/bin/java" ] && JAVA_HOME=$JPATH && break
done
export JAVA_HOME

mvn clean install -am -pl superbi-subscription -f $CURR_DIR/../pom.xml

MVN_STATUS=$?
[ $MVN_STATUS -eq 0 ] || exit $MVN_STATUS

