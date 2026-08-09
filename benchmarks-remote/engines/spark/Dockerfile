FROM python:3.11-bookworm

ARG SPARK_VERSION=4.0.0
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl openjdk-17-jre-headless \
    && pip install --no-cache-dir flask==3.0.0 pyspark==${SPARK_VERSION} \
    && spark_home=$(python -c 'import os,pyspark; print(os.path.dirname(pyspark.__file__))') \
    && curl -fsSL -o "${spark_home}/jars/hadoop-aws-3.4.1.jar" https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    && curl -fsSL -o "${spark_home}/jars/bundle-2.29.52.jar" https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar \
    && curl -fsSL -o "${spark_home}/jars/aws-java-sdk-bundle-1.12.262.jar" https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}
COPY spark_http.py /opt/benchmark/spark_http.py
