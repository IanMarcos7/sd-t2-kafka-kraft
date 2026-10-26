#!/usr/bin/env bash
# E4 e E9 (parte 1): ordem global x ordem por particao x ordem por chave.
# Produz as mensagens 1..N num topico novo de 3 particoes e consome mostrando particao e offset.
#   bash scripts/ordem_chave.sh sem-chave   [N=30]  # particionador padrao (sticky: lotes vao para uma particao)
#   bash scripts/ordem_chave.sh round-robin [N=30]  # sem chave, espalha uma mensagem por particao
#   bash scripts/ordem_chave.sh mesma-chave [N=30]  # todas com a chave "cliente1"
#   bash scripts/ordem_chave.sh chaves      [N=30]  # chaves cliente1, cliente4, cliente10 alternadas
# (com 3 particoes o hash murmur2 leva cliente1/cliente4/cliente10 para P0/P1/P2;
#  cliente1, cliente2 e cliente3 cairiam todas na P0)
source "$(dirname "$0")/lib.sh"
modo=${1:-sem-chave}; n=${2:-30}
topico="ordem-${modo}-$(date +%H%M%S)"
saida="$RES/ordem_${modo}_$(date +%Y%m%d_%H%M%S).txt"
cria_topico "$topico" 3 3

props=(--producer-property acks=all --producer-property linger.ms=0)
case "$modo" in
  sem-chave)   gerar() { seq 1 "$n"; } ;;
  round-robin) gerar() { seq 1 "$n"; }
               props+=(--producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner) ;;
  mesma-chave) gerar() { seq 1 "$n" | sed 's/^/cliente1:/'; }
               props+=(--property parse.key=true --property key.separator=:) ;;
  chaves)      gerar() { seq 1 "$n" | awk 'BEGIN{split("cliente1 cliente4 cliente10",c," ")} {print c[($1-1)%3+1] ":" $1}'; }
               props+=(--property parse.key=true --property key.separator=:) ;;
  *) sed -n '2,10p' "$0"; exit 1 ;;
esac

log "produzindo 1..$n no topico $topico (modo $modo)"
gerar | docker exec -i cliente "$KBIN/kafka-console-producer.sh" --bootstrap-server "$BS" \
          --topic "$topico" "${props[@]}" >/dev/null 2>&1

log "consumindo desde o inicio"
kc -C -t "$topico" -o beginning -e -q -f '%p|%o|%k|%s\n' >"$RES/.consumo.tmp"

{
  echo "Topico: $topico   modo: $modo   mensagens: $n"
  echo
  echo "Ordem em que o consumidor recebeu (particao, offset, chave, valor):"
  awk -F'|' '{printf "  P%s  off=%-3s  chave=%-9s  valor=%s\n", $1, $2, ($3==""?"-":$3), $4}' "$RES/.consumo.tmp"
  echo
  echo "Sequencia por particao (ordenada por offset):"
  sort -t'|' -k1,1n -k2,2n "$RES/.consumo.tmp" |
    awk -F'|' '{seq[$1]=seq[$1] " " $4} END{for(p in seq) print "  P" p ":" seq[p]}' | sort
  echo
  awk -F'|' '{v=$4+0; if(v<ant) g++; ant=v
        if(v<ultp[$1]) p++; ultp[$1]=v
        if($3!=""){ temk=1; if(v<ultk[$3]) k++; ultk[$3]=v }}
       END{ printf "Inversoes na ordem global recebida: %d\n", g
            printf "Inversoes dentro de cada particao: %d\n", p
            if (temk) printf "Inversoes dentro de cada chave: %d\n", k }' "$RES/.consumo.tmp"
} | tee "$saida"
rm -f "$RES/.consumo.tmp"
log "salvo em $saida"
