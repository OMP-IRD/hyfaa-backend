FROM python:3.7-slim
LABEL description="API backend for MGB/HYFAA portal"
LABEL maintainer="Jean Pommier, jean.pommier@pi-geosolutions.fr"
LABEL project-scope="MGB/HYFAA"
LABEL git-repo="https://github.com/OMP-IRD/hyfaa-backend"

COPY requirements.txt /requirements.txt

# install uwsgi (need build-essential & python-dev, see https://uwsgi-docs.readthedocs.io/en/latest/Install.html
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential python3-dev && \
    pip install uwsgi && pip install -r /requirements.txt && \
    apt-get remove -y build-essential python-dev && \
    rm -rf /var/lib/apt/lists/*

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN mkdir -p /hyfaa-backend && \
    groupadd --gid 999 hyfaa && \
    useradd -r -ms /bin/bash --uid 999 --gid 999 hyfaa && \
    chown hyfaa:hyfaa /hyfaa-backend/

EXPOSE 5000

COPY --chown=hyfaa:hyfaa src /hyfaa-backend/app

WORKDIR "/hyfaa-backend/app"

VOLUME ["/hyfaa-scheduler/data"]

USER hyfaa

ENV SCRIPT_CONFIG_PATH="conf/script_config.hjson"

CMD ["uwsgi", "--http", ":5000", "--chdir", "/hyfaa-backend/app", "--wsgi-file", "wsgi.py", "--callable", "app", "--master", "--processes", "4", "--threads", "2", "--uid", "hyfaa", "--stats", "127.0.0.1:9191"]
