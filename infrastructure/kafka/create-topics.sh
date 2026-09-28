#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="kafka:9092"

create_topic() {
  local topic="$1"
  local partitions="$2"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1
}

create_topic woe.workflow.commands 3
create_topic woe.task.commands 6
create_topic woe.task.events 6
create_topic woe.workflow.events 3
create_topic woe.test.contracts 1
