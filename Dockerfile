
FROM python:3.7-buster

WORKDIR /home
COPY resources/requirements/requirements.txt /tmp/requirements.txt
COPY resources/credentials/service_account.json /home/service_account.json

RUN apt-get update \
    && apt-get install -y python-pip \
    && pip install -r /tmp/requirements.txt \
    && rm -rf /var/lib/apt/lists/*

COPY resources/pipeline.py /home/pipeline.py
COPY resources/run-pipeline.sh /home/run-pipeline.sh

ENTRYPOINT [ "/home/run-pipeline.sh" ]


