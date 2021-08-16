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
#ADD spark/log4j.properties /opt/spark/conf/log4j.properties


# Sedona
RUN cd /opt/bitnami/spark/jars && \
    wget "https://search.maven.org/remotecontent?filepath=org/apache/sedona/sedona-python-adapter-3.0_2.12/1.0.1-incubating/sedona-python-adapter-3.0_2.12-1.0.1-incubating.jar" -O "sedona-python-adapter-3.0_2.12-1.0.1-incubating.jar" && \
    wget "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/geotools-24.1/geotools-wrapper-geotools-24.1.jar" -O "geotools-wrapper-geotools-24.1.jar"

COPY image/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf


#RUN echo "spark.driver.bindAddress 127.0.0.1" >> /opt/spark/conf/spark-defaults.conf
#RUN echo "spark.driver.host 127.0.0.1" >> /opt/spark/conf/spark-defaults.conf
COPY target/scala-2.12/engine.spark.jar /opt/engine/bin/engine.spark.jar

# Specify the User that the actual main process will run as
#ARG spark_uid=185
#USER ${spark_uid}
