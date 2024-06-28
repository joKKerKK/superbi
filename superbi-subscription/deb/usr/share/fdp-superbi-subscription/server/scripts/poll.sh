#!/bin/bash

source /usr/share/fdp-superbi-subscription/server/scripts/commons.sh

HTTP_RESPONSE=$(curl --connect-timeout 2 -m 5 --silent --write-out "HTTPSTATUS:%{http_code}" -X GET ${HEALTH_CHECK_URL})
HTTP_BODY=$(echo $HTTP_RESPONSE | sed -e 's/HTTPSTATUS\:.*//g')
HTTP_STATUS=$(echo $HTTP_RESPONSE | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')

if [ "$HTTP_STATUS" == "200" ]; then
    echo "${PACKAGE} is up"
    exit 0
else
    echo "${PACKAGE} is down - Response code was $HTTP_STATUS"
    exit 2
fi
