#!/usr/bin/env bash
# Funcoes comuns do kit. Os outros scripts fazem: source "$(dirname "$0")/lib.sh"
set -uo pipefail

# Git Bash (Windows): nao converter caminhos como /opt/kafka/bin em C:/Program Files/...
export MSYS_NO_PATHCONV=1

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RES="${RES:-$RAIZ/resultados}"   # pode ser trocado: RES=resultados/x bash scripts/...
mkdir -p "$RES"

NOS=(kafka1 kafka2 kafka3)
REDE_INTERNA=t2-kafka_interna
BS="172.31.0.41:19092,172.31.0.42:19092,172.31.0.43:19092"
KBIN=/opt/kafka/bin

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# kafka1 -> 1
id_no() { echo "${1#kafka}"; }
# 1 ou kafka1 -> kafka1
nome_no() { case "$1" in kafka*) echo "$1" ;; *) echo "kafka$1" ;; esac; }
# kafka1 -> 172.30.0.41
ip_interno() { echo "172.30.0.4$(id_no "$(nome_no "$1")")"; }

# Tempo atual em milissegundos
ms() {
  local t; t=$(date +%s%3N 2>/dev/null)
  case "$t" in *N*|"") python3 -c 'import time;print(int(time.time()*1000))' ;; *) echo "$t" ;; esac
}

# Ferramenta do Kafka no conteiner cliente (JVM: demora ~2 s para iniciar)
#   kt kafka-topics.sh --describe --topic pedidos
kt() { local t=$1; shift; docker exec cliente "$KBIN/$t" --bootstrap-server "$BS" "$@"; }

# kcat: consulta de metadados rapida (milissegundos), usada nas medicoes de tempo
kc() { docker exec kcat kcat -b "$BS" "$@"; }

# Lider atual de uma particao (id do no; -1 se nao houver lider)
lider() {
  kc -m 3 -L -t "$1" 2>/dev/null |
    awk -v p="$2" '$1=="partition" && $2==p"," {gsub(",","",$4); print $4; exit}'
}

# ISR atual de uma particao, ex.: 1,2,3
isr() {
  kc -m 3 -L -t "$1" 2>/dev/null |
    awk -v p="$2" '$1=="partition" && $2==p"," {print $NF; exit}'
}

# Verdadeiro se o no (id) esta no ISR da particao
no_isr() { case ",$(isr "$1" "$2")," in *",$3,"*) return 0 ;; *) return 1 ;; esac; }

# Lista de brokers sem um no (para nao perder tempo tentando o no derrubado): bs_sem kafka2
bs_sem() { local id; id=$(id_no "$(nome_no "$1")"); tr ',' '\n' <<<"$BS" | grep -v "\.4$id:" | paste -sd, -; }

# Controlador ativo do quorum KRaft e a epoca: imprime "LeaderId LeaderEpoch"
#   controlador [lista-de-brokers]
controlador() {
  docker exec cliente "$KBIN/kafka-metadata-quorum.sh" --bootstrap-server "${1:-$BS}" describe --status 2>/dev/null |
    awk '/^LeaderId:/{l=$2} /^LeaderEpoch:/{e=$2} END{print l, e}'
}

# Instante (ms desde 1970) em que um no virou lider Raft do quorum, lido do log dele:
# "[2026-10-25 23:33:55,066] INFO [RaftManager id=2] Completed transition to Leader(...)" (UTC)
ts_lider_raft() {
  local l; l=$(docker logs --since 5m "$1" 2>&1 | grep 'Completed transition to Leader' | tail -1 |
               sed -E 's/^\[([0-9-]+ [0-9:]+),([0-9]+)\].*/\1.\2/')
  [ -n "$l" ] && date -u -d "$l" +%s%3N
}

# Fim do log (proximo offset) de uma particao
fim_offset() {
  kc -Q -t "$1:$2:-1" 2>/dev/null | awk '{print $NF; exit}'
}

# Cria topico se nao existir: cria_topico nome particoes [fator]
cria_topico() {
  kt kafka-topics.sh --create --if-not-exists --topic "$1" \
    --partitions "$2" --replication-factor "${3:-3}" >/dev/null
}

# Espera um no voltar a responder depois de docker start
espera_no() {
  local id; id=$(id_no "$(nome_no "$1")")
  for _ in $(seq 1 60); do
    kc -m 2 -L 2>/dev/null | grep -q "broker $id at" && return 0
    sleep 1
  done
  log "aviso: $1 nao voltou em 60 s"
  return 1
}

# Espera todas as particoes de um topico terem ISR completo (3 nos)
espera_isr_completo() {
  for _ in $(seq 1 120); do
    if kc -m 3 -L -t "$1" 2>/dev/null |
       awk '$1=="partition"{n++; if(split($NF,a,",")==3) ok++} END{exit !(n>0 && n==ok)}'; then
      return 0
    fi
    sleep 1
  done
  log "aviso: ISR de $1 nao ficou completo em 120 s"
  return 1
}

# Media e desvio padrao amostral de uma coluna de numeros (stdin)
estat() {
  awk '{s+=$1; q+=$1*$1; n++}
       END{ if(n==0){print "sem dados"; exit}
            m=s/n; sd=(n>1)?sqrt((q-n*m*m)/(n-1)):0; if(sd!=sd) sd=0
            printf "n=%d  media=%.2f  desvio=%.2f\n", n, m, sd }'
}

# Produtor Java "verificavel": imprime uma linha JSON por mensagem (sucesso ou erro).
# Conta os sucessos: vprod <topico> <acks: -1|1|0> <n> <prefixo> [lista-de-brokers]
# Timeouts curtos para nao travar quando o cluster esta degradado.
# VPROD_TIMEOUT (ms) muda o delivery.timeout.ms (padrao 9000); VPROD_LOG guarda as linhas JSON.
vprod() {
  local t=$1 a=$2 n=$3 pre=$4 bs=${5:-$BS} dt=${VPROD_TIMEOUT:-9000}
  docker exec cliente sh -c "printf 'request.timeout.ms=4000\ndelivery.timeout.ms=$dt\nmax.block.ms=9000\nretries=2\n' > /tmp/curto.props"
  timeout $(( dt / 1000 + 50 )) docker exec cliente "$KBIN/kafka-verifiable-producer.sh" --bootstrap-server "$bs" \
      --topic "$t" --max-messages "$n" --acks "$a" --value-prefix "$pre" \
      --producer.config /tmp/curto.props 2>/dev/null | tee -a "${VPROD_LOG:-/dev/null}" |
    grep -c '"name":"producer_send_success"'
}

# Le uma particao desde o inicio ate o high watermark e conta as mensagens (com prefixo opcional)
#   vconta <topico> <particao> [prefixo] [lista-de-brokers]
vconta() {
  local t=$1 p=$2 pre=${3:-} bs=${4:-$BS}
  timeout 25 docker exec kcat kcat -b "$bs" -C -t "$t" -p "$p" -o beginning -e -q -f '%s\n' 2>/dev/null |
    { if [ -n "$pre" ]; then grep -c "^$pre\."; else wc -l; fi; } | tr -d ' '
}

# Lider visto por uma lista especifica de brokers (ex.: so o no isolado)
lider_via() { docker exec kcat kcat -b "$3" -m 3 -L -t "$1" 2>/dev/null |
  awk -v p="$2" '$1=="partition" && $2==p"," {gsub(",","",$4); print $4" isr="$NF; exit}'; }
