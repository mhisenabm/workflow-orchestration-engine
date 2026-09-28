# Docker Infrastructure

The root `Dockerfile` defines separate `runtime` and `test` targets. The root
`docker-compose.yml` wires PostgreSQL, Kafka, one-shot migrations, the three
services, scalable Workers, and the integration-test profile.

Docker files remain at the repository root because those are the conventional
default locations used by `docker build` and `docker compose`; this directory
documents their infrastructure ownership.
