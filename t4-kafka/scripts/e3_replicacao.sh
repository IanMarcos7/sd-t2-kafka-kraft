#!/usr/bin/env bash
# E3 (parte 1): a mesma particao existe nos tres nos, com o mesmo tamanho, e o tempo de
# propagacao ponta a ponta (produtor -> lider -> seguidores -> high watermark -> consumidor).
#   bash scripts/e3_replicacao.sh [repeticoes=5] [mensagens=1000]
source "$(dirname "$0")/lib.sh"
reps=${1:-5}; n=${2:-1000}
saida="$RES/e3_replicacao_$(date +%Y%m%d_%H%M%S).txt"; csv="${saida%.txt}.csv"
cria_topico pedidos 3 3; cria_topico e3-latencia 1 3; espera_isr_completo e3-latencia

e2e() {   # kafka-e2e-latency: produz 1 mensagem por vez e espera o consumidor recebe-la
  local out
  out=$(docker exec cliente "$KBIN/kafka-e2e-latency.sh" "$BS" e3-latencia "$n" "$1" 200 2>&1)
  grep -q -i 'avg latency' <<<"$out" || out=$(docker exec cliente "$KBIN/kafka-e2e-latency.sh" \
      --bootstrap-server "$BS" --topic e3-latencia --num-records "$n" --producer-acks "$1" --record-size 200 2>&1)
  awk -F': *' 'tolower($0) ~ /avg latency/ {gsub(/ ?ms/,"",$2); a=$2}
               /Percentiles/ {match($0,/99th = [0-9.]+/); p=substr($0,RSTART+7,RLENGTH-7)}
               END{print a+0 "," p+0}' <<<"$out"
}
{
  echo "== Diretorios da particao pedidos-* em cada no (docker exec kafkaN ls /tmp/kafka-logs)"
  for nn in "${NOS[@]}"; do echo "  $nn: $(docker exec "$nn" ls /tmp/kafka-logs | grep '^pedidos-' | tr '\n' ' ')"; done
  echo "== Tamanho de cada replica (kafka-log-dirs)"
  kt kafka-log-dirs.sh --describe --topic-list pedidos 2>/dev/null | tail -1 |
    grep -o '"broker":[0-9]*\|"partition":"[^"]*","size":[0-9]*' |
    awk -F'[:,"]+' '/broker/{b=$NF; next} {printf "  broker %s  %s  %s bytes\n", b, $3, $5}'
  echo
  echo "== Latencia ponta a ponta (kafka-e2e-latency, $n mensagens de 200 bytes por rodada)"
  echo "rep,acks,lat_media_ms,p99_ms" >"$csv"
  for a in 1 all; do
    for r in $(seq 1 "$reps"); do
      v=$(e2e "$a"); echo "$r,$a,$v" >>"$csv"; echo "  acks=$a rep $r: media,p99 (ms) = $v"
    done
  done
  for a in 1 all; do echo "  acks=$a media: $(awk -F, -v a="$a" 'NR>1 && $2==a {print $3}' "$csv" | estat)"; done
} 2>&1 | tee "$saida"
log "salvo em $saida e $csv"
