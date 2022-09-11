# Downloading docker image
FROM gcr.io/datamechanics/spark:platform-3.2-latest

# using root user
USER root:root

# create directory for apps
RUN mkdir -p /app

# pip install
RUN pip install --no-cache-dir --upgrade pip

# copy spark program
COPY spark_app_brazilian_subscription.py /app/

# copy jar files
COPY /jars /opt/spark/jars

# set work directory
WORKDIR /app

# user
USER 1001

# command
# Docker build -t spark-py-MYIMAGE .