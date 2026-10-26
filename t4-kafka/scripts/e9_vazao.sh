#!/usr/bin/env bash
# E9 (enfase B): custo da ordem total. Uma particao = ordem total, mas um unico lider recebe tudo.
# Mede a vazao do produtor com 1, 3 e 6 particoes (acks=all), N repeticoes cada.
#   bash scripts/e9_vazao.sh [repeticoes=5] [mensagens=50000] [bytes=200] [particoes="1 3 6"]
source "$(dirname "$0")/lib.sh"
reps=${1:-5}; n=${2:-50000}; tam=${3:-200}; lista=${4:-"1 3 6"}
csv="$RES/e9_vazao_$(date +%Y%m%d_%H%M%S).csv"
echo "particoes,rep,msgs_por_s,mb_por_s,lat_media_ms,lat_max_ms" >"$csv"

for p in $lista; do
  topico="vazao-p$p"
  cria_topico "$topico" "$p" 3
  espera_isr_completo "$topico"
  for r in $(seq 1 "$reps"); do
    log "$p particao(oes), repeticao $r/$reps"
    docker exec cliente "$KBIN/kafka-producer-perf-test.sh" --topic "$topico" \
        --num-records "$n" --record-size "$tam" --throughput -1 \
        --producer-props bootstrap.servers="$BS" acks=all 2>&1 |
      grep 'records sent' | tail -1 |
      awk -F', ' -v p="$p" -v r="$r" '{split($2,x," "); mbs=$2; gsub(/.*\(| MB\/sec\)/,"",mbs)
                                        split($3,l," "); split($4,m," ")
                                        printf "%s,%s,%s,%s,%s,%s\n", p, r, x[1], mbs, l[1], m[1]}' >>"$csv"
    tail -1 "$csv"
  done
done

echo
for p in $lista; do
  echo "$p particao(oes): vazao msgs/s $(awk -F, -v p="$p" 'NR>1 && $1==p {print $3}' "$csv" | estat)"
done
log "salvo em $csv"
