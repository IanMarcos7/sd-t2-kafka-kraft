#!/usr/bin/env bash
# E3: vazao e latencia do produtor (kafka-producer-perf-test), repetido N vezes.
#   bash scripts/medir_produtor.sh [acks=all] [repeticoes=5] [topico=pedidos] [mensagens=50000] [bytes=200]
# Exemplo do roteiro: bash scripts/medir_produtor.sh 1 ; bash scripts/medir_produtor.sh all
source "$(dirname "$0")/lib.sh"
acks=${1:-all}; reps=${2:-5}; topico=${3:-pedidos}; n=${4:-50000}; tam=${5:-200}
csv="$RES/produtor_acks-${acks}_${topico}_$(date +%Y%m%d_%H%M%S).csv"
echo "rep,acks,topico,mensagens,bytes,msgs_por_s,mb_por_s,lat_media_ms,lat_max_ms,p99_ms" >"$csv"

cria_topico "$topico" 3 3
for r in $(seq 1 "$reps"); do
  log "repeticao $r/$reps (acks=$acks)"
  linha=$(docker exec cliente "$KBIN/kafka-producer-perf-test.sh" --topic "$topico" \
            --num-records "$n" --record-size "$tam" --throughput -1 \
            --producer-props bootstrap.servers="$BS" acks="$acks" 2>&1 |
          grep 'records sent' | tail -1)
  # "50000 records sent, 9123.4 records/sec (1.74 MB/sec), 540.2 ms avg latency, 900.0 ms max latency, ... 99th, ..."
  echo "$linha" | awk -F', ' -v r="$r" -v a="$acks" -v t="$topico" -v n="$n" -v b="$tam" '{
      split($2,x," "); mbs=$2; gsub(/.*\(| MB\/sec\)/,"",mbs)
      split($3,l," "); split($4,m," "); p99=""
      for(i=5;i<=NF;i++) if($i ~ /99th/){split($i,p," "); p99=p[1]}
      printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", r,a,t,n,b,x[1],mbs,l[1],m[1],p99 }' >>"$csv"
  tail -1 "$csv"
done

echo
echo "acks=$acks  vazao (msgs/s):   $(tail -n +2 "$csv" | cut -d, -f6 | estat)"
echo "acks=$acks  latencia media (ms): $(tail -n +2 "$csv" | cut -d, -f8 | estat)"
log "salvo em $csv"
