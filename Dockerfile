FROM debian:jessie

ENV APP=/app

RUN \
    echo "deb http://ftp.pl.debian.org/debian/ unstable main contrib non-free" > /etc/apt/sources.list.d/sid.list && \
    apt-get update && \
    apt-get install -y  git-core \
                        golang \
                        dh-golang \
                        && \
    mkdir -p ${APP}

COPY scripts/ /bin/

WORKDIR ${APP}

ENTRYPOINT ["docker-entrypoint.sh"]
