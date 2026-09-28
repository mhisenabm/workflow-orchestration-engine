#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  docker compose down -v --remove-orphans
}
trap cleanup EXIT

# Integration tests always start from fresh database volumes.
docker compose down -v --remove-orphans
docker compose up -d --build --scale worker=3 api engine worker

docker compose --profile test run --rm integration-tests
