# Downloading docker image
#FROM gcr.io/spark-operator/spark-py:v3.0.0
FROM gcr.io/spark-operator/spark-py:v3.1.1-hadoop3

# root user
USER root:root

# create Folder apps
RUN mkdir -p /app

# pip install
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir delta-spark==2.0.0 && \
    pip install --no-cache-dir pytz

# copy spark program
COPY spark_app_brazilian_subscription.py /app/

# copy jar files
COPY ./jars/ /opt/spark/jars

# set work directory
WORKDIR /app

# user
USER 1001

# command
# Docker build -t spark-py-MYIMAGE .


