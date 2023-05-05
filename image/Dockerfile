# Base source: https://github.com/bitnami/bitnami-docker-spark/blob/master/3/debian-10/Dockerfile
ARG BASE_IMAGE
FROM $BASE_IMAGE

# Reset to root to run installation tasks
USER 0


# Tools
RUN apt-get update && \
    apt-get install -y wget unzip tini procps jq && \
    rm -rf /root/.cache && rm -rf /var/cache/apt/* && \
    wget -O /usr/local/bin/yq "https://github.com/mikefarah/yq/releases/download/2.4.1/yq_linux_amd64" && \
    chmod +x /usr/local/bin/yq


# Logging
COPY image/spark.log4j.properties /opt/bitnami/spark/conf/log4j.properties


# Sedona
RUN cd /opt/bitnami/spark/jars && \
    wget "https://search.maven.org/remotecontent?filepath=org/apache/sedona/sedona-python-adapter-3.0_2.12/1.0.1-incubating/sedona-python-adapter-3.0_2.12-1.0.1-incubating.jar" -O "sedona-python-adapter-3.0_2.12-1.0.1-incubating.jar" && \
    wget "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/geotools-24.1/geotools-wrapper-geotools-24.1.jar" -O "geotools-wrapper-geotools-24.1.jar"

COPY image/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf


# Livy
# TODO: Waiting for the next Livy release that supports Scala 2.12
# wget "https://downloads.apache.org/incubator/livy/0.7.1-incubating/apache-livy-0.7.1-incubating-bin.zip" -O "livy.zip"
COPY image/apache-livy-0.8.0-kamu-bin.zip /opt/livy.zip
RUN cd /opt && \
    unzip livy.zip && rm livy.zip && mv apache-livy* livy && \
    mkdir /opt/livy/logs

COPY image/livy.log4j.properties /opt/livy/conf/log4j.properties
COPY image/livy.conf /opt/livy/conf/livy.conf


# Engine
#RUN echo "spark.driver.bindAddress 127.0.0.1" >> /opt/spark/conf/spark-defaults.conf
#RUN echo "spark.driver.host 127.0.0.1" >> /opt/spark/conf/spark-defaults.conf
COPY target/scala-2.12/engine.spark.jar /opt/engine/bin/engine.spark.jar


# Adapter
COPY adapter/target/x86_64-unknown-linux-musl/release/kamu-engine-spark-adapter /opt/engine/bin/adapter


EXPOSE 2884/tcp
ENTRYPOINT ["/usr/bin/tini", "--"]
ENV RUST_BACKTRACE=1
CMD ["/opt/engine/bin/adapter"]

# Restore base image's run config
# WORKDIR /opt/bitnami/spark
# USER 1001
# ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]
# CMD [ "/opt/bitnami/scripts/spark/run.sh" ]
