FROM metricq/metricq-python:latest AS builder
LABEL maintainer="mario.bielert@tu-dresden.de"

USER root
RUN apt-get update && apt-get install -y git wget

USER metricq
COPY --chown=metricq:metricq . /home/metricq/grafana

WORKDIR /home/metricq/grafana
RUN . /home/metricq/venv/bin/activate && pip install .
RUN wget -O wait-for-it.sh https://github.com/vishnubob/wait-for-it/raw/master/wait-for-it.sh && chmod +x wait-for-it.sh

FROM metricq/metricq-python:latest

USER metricq
COPY --from=builder /home/metricq/venv /home/metricq/venv
COPY --from=builder /home/metricq/grafana/wait-for-it.sh /home/metricq/wait-for-it.sh

ARG wait_for_rabbitmq_url=127.0.0.1:5672
ENV wait_for_rabbitmq_url=$wait_for_rabbitmq_url

ARG metricq_url=amqp://localhost:5672
ENV metricq_url=$metricq_url

CMD /home/metricq/wait-for-it.sh $wait_for_rabbitmq_url -- /home/metricq/venv/bin/metricq-grafana $metricq_url
