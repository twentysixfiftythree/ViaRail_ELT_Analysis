FROM bitnami/spark:3.5.1
USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
RUN install_packages curl

# Download GCS connector jar
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar \
    -o /opt/bitnami/spark/jars/gcs-connector-hadoop3-2.2.5.jar

RUN curl -L https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.42.2/spark-bigquery-with-dependencies_2.12-0.42.2.jar \
    -o /opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.42.2.jar
  


# download missing dependency (javax.inject)
RUN curl -L https://repo1.maven.org/maven2/javax/inject/javax.inject/1/javax.inject-1.jar \
    -o /opt/bitnami/spark/jars/javax.inject-1.jar

USER 1001