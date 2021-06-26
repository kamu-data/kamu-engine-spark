ARG BASE_IMAGE
FROM $BASE_IMAGE

# Reset to root to run installation tasks
USER 0

RUN echo "spark.driver.bindAddress 127.0.0.1" >> /opt/spark/conf/spark-defaults.conf
RUN echo "spark.driver.host 127.0.0.1" >> /opt/spark/conf/spark-defaults.conf
ADD target/scala-2.12/engine.spark.jar /opt/engine/bin/engine.spark.jar

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
