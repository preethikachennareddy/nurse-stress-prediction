
FROM maven:3.9.2-eclipse-temurin-17

ENV FLINK_HOST=jobmanager
ENV FLINK_PORT=8081

WORKDIR /app

COPY pom.xml .
COPY src ./src

RUN mvn clean package

ENV FLINK_VERSION=1.19.3
RUN apt-get update && apt-get install -y curl tar \
    && curl -L https://downloads.apache.org/flink/flink-$FLINK_VERSION/flink-$FLINK_VERSION-bin-scala_2.12.tgz -o /tmp/flink.tgz \
    && tar -xzf /tmp/flink.tgz -C /opt/ \
    && rm /tmp/flink.tgz
ENV PATH=$PATH:/opt/flink-$FLINK_VERSION/bin

COPY submit-job.sh /app/submit-job.sh
RUN chmod +x /app/submit-job.sh

CMD ["/app/submit-job.sh"]
