#!/usr/bin/env bash
# Injecao de falhas (E5, E6, E7, E8).
#   bash scripts/falha.sh parar      kafka2     # docker stop (desligamento limpo)
#   bash scripts/falha.sh matar      kafka2     # docker kill (queda abrupta)
#   bash scripts/falha.sh iniciar    kafka2     # docker start
#   bash scripts/falha.sh pausar     kafka2     # congela o processo (docker pause)
#   bash scripts/falha.sh retomar    kafka2
#   bash scripts/falha.sh isolar     kafka2     # tira da rede interna (continua na rede de clientes)
#   bash scripts/falha.sh reconectar kafka2
#   bash scripts/falha.sh lider      pedidos 0  # mostra o lider da particao 0
#   bash scripts/falha.sh controlador           # mostra LeaderId e LeaderEpoch do quorum
source "$(dirname "$0")/lib.sh"

acao=${1:-}; alvo=${2:-}
[ -n "$alvo" ] && [ "$acao" != lider ] && alvo=$(nome_no "$alvo")

case "$acao" in
  parar)      docker stop "$alvo" ;;
  matar)      docker kill "$alvo" ;;
  iniciar)    docker start "$alvo" && espera_no "$alvo" ;;
  pausar)     docker pause "$alvo" ;;
  retomar)    docker unpause "$alvo" ;;
  isolar)     docker network disconnect "$REDE_INTERNA" "$alvo" ;;
  reconectar) docker network connect --ip "$(ip_interno "$alvo")" "$REDE_INTERNA" "$alvo" ;;
  lider)      echo "lider de ${2:?topico}/${3:?particao}: $(lider "$2" "$3")  ISR: $(isr "$2" "$3")" ;;
  controlador) controlador | awk '{print "LeaderId="$1"  LeaderEpoch="$2}' ;;
  *) sed -n '2,12p' "$0"; exit 1 ;;
esac
log "$acao ${alvo:-} ok"
