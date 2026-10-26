#!/usr/bin/env bash
# Mostra o estado do cluster (evidencia do E1). Salva copia em resultados/.
#   bash scripts/status.sh [topico]
source "$(dirname "$0")/lib.sh"
cd "$RAIZ" || exit 1
topico=${1:-}
saida="$RES/status_$(date +%Y%m%d_%H%M%S).txt"

{
  echo "===== Conteineres"
  docker compose ps --format 'table {{.Name}}\t{{.Status}}'
  echo
  echo "===== Quorum KRaft (kafka-metadata-quorum describe --status)"
  kt kafka-metadata-quorum.sh describe --status
  echo
  echo "===== Topicos (kafka-topics --describe)"
  if [ -n "$topico" ]; then
    kt kafka-topics.sh --describe --topic "$topico"
  else
    kt kafka-topics.sh --describe --exclude-internal
  fi
} 2>&1 | tee "$saida"

log "salvo em $saida"
