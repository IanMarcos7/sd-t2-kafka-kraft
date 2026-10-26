#!/usr/bin/env bash
# E6 / E7: isola um no da rede interna, produz enquanto ele esta fora, reconecta e mede
# o tempo ate ele voltar ao ISR (catch-up pelo log). Repetido N vezes.
#   bash scripts/medir_recuperacao.sh [no=kafka3] [repeticoes=5] [topico=pedidos] [particao=0] [mensagens=20000]
# O no so sai do ISR depois de replica.lag.time.max.ms (30 s por padrao), entao cada repeticao leva ~1 min.
source "$(dirname "$0")/lib.sh"
no=$(nome_no "${1:-kafka3}"); reps=${2:-5}; topico=${3:-pedidos}; part=${4:-0}; n=${5:-20000}
id=$(id_no "$no")
csv="$RES/recuperacao_${no}_$(date +%Y%m%d_%H%M%S).csv"
echo "rep,no,topico,particao,isr_isolado,tempo_saida_isr_ms,mensagens_durante,tempo_volta_isr_ms" >"$csv"

cria_topico "$topico" 3 3
# o no isolado responde com metadados antigos (ainda se ve no ISR): consulta so os outros dois
outros=$(bs_sem "$no")
isr_maj() { lider_via "$topico" "$part" "$outros" | sed 's/.*isr=//'; }
no_isr_maj() { case ",$(isr_maj)," in *",$id,"*) return 0 ;; *) return 1 ;; esac; }
for r in $(seq 1 "$reps"); do
  espera_isr_completo "$topico"
  log "rep $r/$reps: isolando $no da rede interna"
  t0=$(ms)
  docker network disconnect "$REDE_INTERNA" "$no"
  while no_isr_maj; do sleep 0.5; done
  t_saida=$(( $(ms) - t0 ))
  isr_iso=$(isr_maj)
  log "  $no saiu do ISR apos ${t_saida} ms (ISR agora: $isr_iso); produzindo $n mensagens"
  docker exec cliente "$KBIN/kafka-producer-perf-test.sh" --topic "$topico" --num-records "$n" \
    --record-size 200 --throughput -1 --producer-props bootstrap.servers="$outros" acks=all >/dev/null 2>&1

  log "  reconectando $no"
  t1=$(ms)
  docker network connect --ip "$(ip_interno "$no")" "$REDE_INTERNA" "$no"
  until no_isr_maj; do sleep 0.1; done
  t_volta=$(( $(ms) - t1 ))
  echo "$r,$no,$topico,$part,\"$isr_iso\",$t_saida,$n,$t_volta" | tee -a "$csv"
done

echo
echo "tempo para voltar ao ISR (ms): $(tail -n +2 "$csv" | awk -F, '{print $NF}' | estat)"
log "salvo em $csv"
