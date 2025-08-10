FROM bitnami/spark:latest

# switch to root to install packages
USER root

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

RUN install_packages curl

#  bitnami uses 1001 as the default user
USER 1001

RUN curl https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar --output /opt/bitnami/spark/jars/gcs-connector-hadoop3-2.2.5.jar
