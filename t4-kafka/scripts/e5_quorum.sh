#!/usr/bin/env bash
# E5 (parte 2): quadro "nos ativos x leitura x escrita (acks=all) x escrita (acks=1) x metadados".
# Usa um topico de 1 particao (fator 3) e derruba primeiro um, depois dois nos que NAO sao o lider,
# para que o lider continue de pe e o efeito do min.insync.replicas=2 e do quorum KRaft apareca.
#   bash scripts/e5_quorum.sh
source "$(dirname "$0")/lib.sh"
t="e5-quorum-$(date +%H%M%S)"
saida="$RES/e5_quorum_$(date +%Y%m%d_%H%M%S).txt"
csv="${saida%.txt}.csv"
cria_topico "$t" 1 3; espera_isr_completo "$t"
L=$(lider "$t" 0)
outros=(); for n in 1 2 3; do [ "$n" != "$L" ] && outros+=("$n"); done
log "topico $t: lider kafka$L; vou derrubar kafka${outros[0]} e depois kafka${outros[1]}"
echo "nos_ativos,lider,isr,escrita_acks_all_ok_de_10,escrita_acks_1_ok_de_10,lidas_ate_hw,metadados_criar_topico,controlador" >"$csv"

estado() {
  local rot=$1 ativos=$2 lid isr_ all um lidas meta ctl
  lid=$(lider "$t" 0); isr_=$(isr "$t" 0)
  all=$(vprod "$t" -1 10 "${ativos}1")
  um=$(vprod "$t" 1 10 "${ativos}2")
  lidas=$(vconta "$t" 0)
  if timeout 40 docker exec cliente "$KBIN/kafka-topics.sh" --bootstrap-server "$BS" --create \
       --topic "$t-meta-$ativos" --partitions 1 --replication-factor 1 >/dev/null 2>&1; then meta=ok; else meta=falhou; fi
  ctl=$(timeout 30 docker exec cliente "$KBIN/kafka-metadata-quorum.sh" --bootstrap-server "$BS" describe --status 2>/dev/null |
        awk '/^LeaderId:/{l=$2} /^LeaderEpoch:/{e=$2} END{ if(l=="") print "sem_resposta"; else print "LeaderId=" l " epoca=" e }')
  echo "$ativos,$lid,\"$isr_\",$all,$um,$lidas,$meta,$ctl" | tee -a "$csv"
}

{
  echo "== 3 nos ativos"; estado tres 3
  log "parando kafka${outros[0]}"; docker stop "kafka${outros[0]}" >/dev/null; sleep 12
  echo "== 2 nos ativos (kafka${outros[0]} parado)"; estado dois 2
  log "parando kafka${outros[1]}"; docker stop "kafka${outros[1]}" >/dev/null; sleep 12
  echo "== 1 no ativo (so kafka$L)"; estado um 1
  log "religando os dois nos"
  docker start "kafka${outros[0]}" "kafka${outros[1]}" >/dev/null
  espera_no "kafka${outros[0]}"; espera_no "kafka${outros[1]}"; espera_isr_completo "$t"
  echo "== depois de religar: mensagens na particao (ate o HW), por estado de origem"
  for a in 3 2 1; do
    echo "  escritas com $a no(s) ativo(s): acks=all -> $(vconta "$t" 0 "${a}1") lidas; acks=1 -> $(vconta "$t" 0 "${a}2") lidas"
  done
  echo "  total na particao: $(vconta "$t" 0)"
} 2>&1 | tee "$saida"
log "salvo em $saida e $csv"
