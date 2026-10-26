#!/usr/bin/env bash
# Sobe o cluster do zero, espera o quorum KRaft e cria o topico "pedidos" (E1).
#   bash scripts/iniciar.sh
source "$(dirname "$0")/lib.sh"
cd "$RAIZ" || exit 1

log "subindo os conteineres"
docker compose up -d || exit 1

log "esperando o quorum de controladores (3 votantes)"
for i in $(seq 1 90); do
  st=$(kt kafka-metadata-quorum.sh describe --status 2>/dev/null)
  votantes=$(grep -o '"id": [0-9]' <<<"$st" | sort -u | wc -l)
  if grep -q '^LeaderId:' <<<"$st" && [ "$votantes" -ge 3 ]; then
    log "quorum formado: $(controlador | awk '{print "LeaderId="$1", LeaderEpoch="$2}')"
    break
  fi
  [ "$i" -eq 90 ] && { log "erro: quorum nao se formou"; exit 1; }
  sleep 2
done

log "criando o topico pedidos (3 particoes, fator 3)"
cria_topico pedidos 3 3
espera_isr_completo pedidos

bash "$RAIZ/scripts/status.sh"
