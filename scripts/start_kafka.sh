#!/usr/bin/env bash
# Start a single-node Kafka (KRaft mode, no ZooKeeper) for local development and CI.
# Needs Java 17+. Usage: scripts/start_kafka.sh [install_dir]
set -euo pipefail
DIR="${1:-$HOME/kafka-local}"
VER=3.7.1
mkdir -p "$DIR" && cd "$DIR"
if [ ! -d "kafka_2.13-$VER" ]; then
  curl -fsSL -o kafka.tgz "https://archive.apache.org/dist/kafka/$VER/kafka_2.13-$VER.tgz"
  tar xzf kafka.tgz
fi
K="$DIR/kafka_2.13-$VER"
cat > kraft.properties <<CFG
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$DIR/data
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
CFG
rm -rf "$DIR/data"
"$K/bin/kafka-storage.sh" format -t "$("$K/bin/kafka-storage.sh" random-uuid)" -c kraft.properties >/dev/null
KAFKA_HEAP_OPTS="-Xmx1g" nohup "$K/bin/kafka-server-start.sh" kraft.properties > kafka.log 2>&1 &
for _ in $(seq 1 60); do
  "$K/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && { echo "Kafka is up on localhost:9092"; exit 0; }
  sleep 2
done
echo "Kafka did not start; see $DIR/kafka.log" >&2; exit 1
