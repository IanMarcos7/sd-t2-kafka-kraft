#!/usr/bin/env bash
cd "$(dirname "$0")/../.."
source scripts/lib.sh
{
echo "== ls kafka-native"; docker exec kafka1 ls /tmp/kafka-logs 2>&1 | head -20
echo "== cat checkpoint"; docker exec kafka1 sh -c 'cat /tmp/kafka-logs/pedidos-0/leader-epoch-checkpoint' 2>&1
echo "== bins"; docker exec cliente ls /opt/kafka/bin | tr '\n' ' '
echo; echo "== log-dirs"; kt kafka-log-dirs.sh --describe --topic-list pedidos 2>&1 | tail -1 | cut -c1-600
echo; echo "== get-offsets"; kt kafka-get-offsets.sh --topic pedidos 2>&1
echo "== timeout host"; which timeout
echo "== kcat produce acks"; echo "x" | docker exec -i kcat kcat -b "$BS" -P -t pedidos -p 0 -X acks=all -X message.timeout.ms=5000; echo "rc=$?"
echo "== docker stats"; docker stats --no-stream --format '{{.Name}} {{.MemUsage}}'
echo "== date"; date -u +%s%3N; docker exec kafka1 date -u +%s 2>&1
} > resultados/testes_do_kit/sonda.log 2>&1
echo FIM >> resultados/testes_do_kit/sonda.log
