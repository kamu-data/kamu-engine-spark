# Source:
# https://hub.docker.com/r/bitnami/spark/
# https://github.com/bitnami/containers/tree/main/bitnami/spark
ARG BASE_IMAGE
FROM $BASE_IMAGE
ARG TARGETPLATFORM

# Reset to root to run installation tasks
USER 0


# Tools
RUN apt-get update && \
    apt-get install -y wget unzip tini procps && \
    apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives


# Logging
COPY image/spark.log4j.properties /opt/bitnami/spark/conf/log4j.properties


# Sedona
RUN cd /opt/bitnami/spark/jars && \
    wget "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.5.1/sedona-spark-shaded-3.5_2.12-1.5.1.jar" -O "sedona-spark-shaded-3.5_2.12-1.5.1.jar" && \
    wget "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.5.1-28.2/geotools-wrapper-1.5.1-28.2.jar" -O "geotools-wrapper-1.5.1-28.2.jar"

COPY image/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf


# Livy
# TODO: Livy is poorly maintained, so we are building our own version from a fork (see DEVELOPER.md for details)
# wget "https://dlcdn.apache.org/incubator/livy/0.8.0-incubating/apache-livy-0.8.0-incubating_2.12-bin.zip" -O "livy.zip"
COPY image/apache-livy-0.9.0-incubating-SNAPSHOT_2.12-bin.zip /opt/livy.zip
RUN cd /opt && \
    unzip livy.zip && rm livy.zip && mv apache-livy* livy && \
    mkdir /opt/livy/logs

COPY image/livy.log4j.properties /opt/livy/conf/log4j.properties
COPY image/livy.conf /opt/livy/conf/livy.conf

# Java 17 madness
# See: https://stackoverflow.com/questions/72724816/running-unit-tests-with-spark-3-3-0-on-java-17-fails-with-illegalaccesserror-cl
ENV LIVY_SERVER_JAVA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"


# Engine
COPY target/scala-2.12/engine.spark.jar /opt/engine/bin/engine.spark.jar


# Adapter
COPY image/tmp/$TARGETPLATFORM/adapter /opt/engine/bin/adapter


EXPOSE 2884/tcp
ENTRYPOINT ["/usr/bin/tini", "--"]
ENV RUST_BACKTRACE=1
CMD ["/opt/engine/bin/adapter"]
