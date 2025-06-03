FROM python:3.11-slim

WORKDIR /app

ENV KAFKA_VERSION 3.7.2
ENV KAFKA_SCALA_VERSION 2.13
ENV KAFKA_HOME /app/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}
ENV PATH $KAFKA_HOME/bin:$PATH

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    curl -L -o kafka.tgz "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz" && \
    tar -xzf kafka.tgz && \
    rm kafka.tgz && \
    chmod +x $KAFKA_HOME/bin/*.sh

# Install required libraries
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x ./reset_kafka.sh

ENTRYPOINT ["./reset_kafka.sh"]