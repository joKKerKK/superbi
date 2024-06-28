FROM jfrog.fkinternal.com/fk-base-images/java/jre:8u202-debian11.3-slim-20220527
MAINTAINER FDP Ssi Dev <fdp-ssi-dev@flipkart.com>

EXPOSE 21212 21218

COPY --chown=root:root . /

RUN sed -i 's/security.debian.org/10.19.1.12/g' /etc/apt/sources.list
RUN sed -i 's/deb.debian.org/10.19.1.12/g' /etc/apt/sources.list
RUN apt-get update

RUN bash /setup.sh && \
    rm /setup.sh

ENTRYPOINT ["/usr/share/fdp-superbi-brv2/server/bin/entry.sh"]
