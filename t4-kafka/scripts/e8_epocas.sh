#!/usr/bin/env bash
# E8 (parte 2): epoca do lider de particao (leader epoch) e epoca do quorum KRaft.
# Mostra o arquivo leader-epoch-checkpoint em cada no (epoca -> offset inicial), troca o lider
# e mostra a epoca nova. E esse numero que impede um lider antigo de continuar escrevendo (fencing).
#   bash scripts/e8_epocas.sh [topico=pedidos] [particao=0]
source "$(dirname "$0")/lib.sh"
t=${1:-pedidos}; p=${2:-0}
saida="$RES/e8_epocas_$(date +%Y%m%d_%H%M%S).txt"
cria_topico "$t" 3 3; espera_isr_completo "$t"

checkpoints() {
  for n in "${NOS[@]}"; do
    printf '  %s: ' "$n"
    docker exec "$n" sh -c "cat /tmp/kafka-logs/$t-$p/leader-epoch-checkpoint" 2>/dev/null |
      awk 'NR>2{printf "[epoca %s desde offset %s] ", $1, $2} END{print ""}'
  done
}
{
  echo "Quorum KRaft antes: $(controlador | awk '{print "LeaderId="$1", epoca="$2}')"
  L=$(lider "$t" "$p")
  echo "Lider de $t/$p antes: kafka$L. Historico de epocas (leader-epoch-checkpoint):"
  checkpoints
  vprod "$t" -1 20 8 >/dev/null
  log "parando kafka$L"
  docker stop "kafka$L" >/dev/null
  for _ in $(seq 1 60); do n=$(lider "$t" "$p"); [ -n "$n" ] && [ "$n" != "$L" ] && [ "$n" != -1 ] && break; sleep 0.5; done
  echo
  echo "Novo lider de $t/$p: kafka$n. Uma escrita para registrar a nova epoca: $(vprod "$t" -1 5 9) de 5"
  sleep 10   # os seguidores gravam o leader-epoch-checkpoint de forma assincrona
  checkpoints
  docker start "kafka$L" >/dev/null; espera_no "kafka$L"; espera_isr_completo "$t"
  vprod "$t" -1 5 9 >/dev/null; sleep 10
  echo
  echo "Depois que kafka$L voltou. Obs.: os seguidores atualizam a epoca em memoria e gravam este"
  echo "arquivo de forma preguicosa; a fonte de verdade e o checkpoint do lider:"
  checkpoints
  echo "Quorum KRaft depois: $(controlador | awk '{print "LeaderId="$1", epoca="$2}')"
} 2>&1 | tee "$saida"
log "salvo em $saida"
