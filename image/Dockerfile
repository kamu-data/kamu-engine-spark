ARG BASE_IMAGE
FROM $BASE_IMAGE

ADD image/entrypoint.sh /opt/entrypoint.sh
ADD image/config.json /opt/transform/config.json
ADD target/scala-2.11/transform-streaming-assembly-*.jar /opt/transform/transform-streaming-assembly.jar
