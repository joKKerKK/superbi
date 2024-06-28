#!/bin/bash -ex
PACKAGE=__PACKAGE__

. /usr/share/$PACKAGE/server/commons.sh

FK_USER=$PACKAGE
FK_GROUP=fdp

addgroup --gid 914299 $FK_GROUP && \
adduser --gecos "FK" --gid 914299 --uid 914299 --no-create-home --shell /bin/bash --disabled-login $FK_USER

[ ! -d "$LOG_DIR" ] && mkdir -p "$LOG_DIR"
chown -Rf $FK_USER:$FK_GROUP "$LOG_DIR" || true

chown -Rf $FK_USER:$FK_GROUP $BASE_DIR
chown -Rf $FK_USER:$FK_GROUP $WORK_DIR

chmod -R +x $SERVER_DIR/

apt-get update -q -q
apt-get install --yes --allow-unauthenticated vim less




