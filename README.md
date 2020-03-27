![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)
# metricq-grafana

A MetricQ sink, which can anwser requests of the grafana-metricq-source.

## Deployment with Docker

### Prerequisites
This needs the metricq-python docker image present on the deployment system
- run `make docker` in MetricQ main repository

### Build an image

The configuration is passed as build arguments:

```
docker build -t metricq-grafana --build-arg metricq_url=amqp://localhost --build-arg wait_for_rabbitmq_url=localhost:5672 .
```

### Create a new container from the image

```
docker run -it --restart=unless-stopped metricq-grafana
```

> For restart policies see: https://docs.docker.com/engine/reference/commandline/run/#restart-policies---restart
