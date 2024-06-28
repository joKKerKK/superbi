#!/bin/bash

source /usr/share/fdp-superbi-subscription/server/scripts/commons.sh

sleep 300
exec 2>&1
LOG INFO "Sending ${PACKAGE} server health check" >> /var/log/nsca.log
exec /usr/lib/nagios/plugins/fk-nsca-wrapper/nsca_wrapper -H `hostname -f | sed -r 's#\.(nm|ch)\.flipkart\.com##g'` -S "${PACKAGE}-server" -b /usr/sbin/send_nsca -c /etc/send_nsca.cfg -C "${POLLER_FILE} -w 85 -c 88 -x /dev/shm -x /dev -x /boot -x /lib/init/rw" >> /var/log/nsca.log 2>&1