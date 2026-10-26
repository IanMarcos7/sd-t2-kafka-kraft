#!/usr/bin/env bash
# E9 (enfase B): efeito de retries, max.in.flight.requests.per.connection e idempotencia na ordem.
# Produz 1..N com a mesma chave numa particao, derruba o lider no meio da producao e confere
# no consumo: inversoes de ordem, duplicatas e mensagens perdidas.
#
#   bash scripts/e9_retries.sh <perfil> [N=300000] [falha=matar|parar|isolar|pausar]
#
# Perfis (producer acks=all em todos):
#   sem-idemp-5   enable.idempotence=false, max.in.flight=5, retries=MAX  -> pode inverter e duplicar
#   sem-idemp-1   enable.idempotence=false, max.in.flight=1, retries=MAX  -> ordem mantida, pode duplicar
#   idemp-5       enable.idempotence=true,  max.in.flight=5               -> ordem mantida, sem duplicatas
#   sem-retries   enable.idempotence=false, retries=0                      -> pode perder mensagens
#   todos         roda os quatro perfis em sequencia
source "$(dirname "$0")/lib.sh"
perfil=${1:-}; n=${2:-300000}; falha=${3:-matar}

if [ "$perfil" = todos ]; then
  for p in sem-idemp-5 sem-idemp-1 idemp-5 sem-retries; do bash "$0" "$p" "$n" "$falha"; done
  exit 0
fi

case "$perfil" in
  sem-idemp-5) cfg="enable.idempotence=false max.in.flight.requests.per.connection=5 retries=2147483647" ;;
  sem-idemp-1) cfg="enable.idempotence=false max.in.flight.requests.per.connection=1 retries=2147483647" ;;
  idemp-5)     cfg="enable.idempotence=true max.in.flight.requests.per.connection=5 retries=2147483647" ;;
  sem-retries) cfg="enable.idempotence=false max.in.flight.requests.per.connection=5 retries=0" ;;
  *) sed -n '2,16p' "$0"; exit 1 ;;
esac

topico="e9-${perfil}-$(date +%H%M%S)"
cria_topico "$topico" 1 3
espera_isr_completo "$topico"
lider0=$(lider "$topico" 0); alvo=kafka$lider0

props=(--property parse.key=true --property key.separator=:
       --producer-property acks=all --producer-property linger.ms=1 --producer-property batch.size=2048
       --producer-property request.timeout.ms=3000 --producer-property delivery.timeout.ms=60000
       --producer-property retry.backoff.ms=50)
for kv in $cfg; do props+=(--producer-property "$kv"); done

log "perfil $perfil: $cfg"
log "produzindo 1..$n (chave fixa) em $topico; lider da particao = $alvo"
seq 1 "$n" | sed 's/^/k:/' |
  docker exec -i cliente "$KBIN/kafka-console-producer.sh" --bootstrap-server "$BS" \
    --topic "$topico" "${props[@]}" >"$RES/.produtor.log" 2>&1 &
pid=$!

# derruba o lider quando ~1/4 das mensagens ja chegou
alvo_off=$((n / 4)); t0=$(ms)
while kill -0 "$pid" 2>/dev/null; do
  off=$(fim_offset "$topico" 0); off=${off:-0}
  [ "$off" -ge "$alvo_off" ] 2>/dev/null && break
  sleep 0.2
done
off_falha=$(fim_offset "$topico" 0)
log "falha ($falha) em $alvo com offset ${off_falha:-?} apos $(( $(ms) - t0 )) ms"
case "$falha" in
  matar)  docker kill "$alvo" >/dev/null ;;
  parar)  docker stop "$alvo" >/dev/null ;;
  isolar) docker network disconnect "$REDE_INTERNA" "$alvo" ;;
  pausar) docker pause "$alvo" >/dev/null ;;
esac
wait "$pid"
log "producao terminou; novo lider: kafka$(lider "$topico" 0)"

log "restaurando $alvo"
case "$falha" in
  isolar) docker network connect --ip "$(ip_interno "$alvo")" "$REDE_INTERNA" "$alvo" ;;
  pausar) docker unpause "$alvo" >/dev/null ;;
  *)      docker start "$alvo" >/dev/null ;;
esac
espera_no "$alvo"

log "consumindo e conferindo"
saida="$RES/e9_retries_${perfil}_$(date +%Y%m%d_%H%M%S).txt"
erros=$(grep -c -i -E 'error|exception' "$RES/.produtor.log" || true)
kc -C -t "$topico" -p 0 -o beginning -e -q -f '%s\n' |
  awk -v n="$n" -v perfil="$perfil" -v cfg="$cfg" -v falha="$falha" -v alvo="$alvo" -v off="$off_falha" -v erros="$erros" '
    { v=$1+0; tot++
      if (seen[v]++) { dup++; next }          # repeticao de valor ja recebido
      if (v < max) inv++                      # valor novo chegando depois de um maior: fora de ordem
      if (v > max) max=v }
    END {
      for (i=1; i<=n; i++) if (!(i in seen)) perd++
      printf "perfil=%s\nconfig: %s\nfalha=%s no lider %s (offset %s)\n", perfil, cfg, falha, alvo, off
      printf "enviadas=%d  recebidas=%d\n", n, tot
      printf "fora de ordem (valores novos apos um maior)=%d\nduplicatas=%d\nperdidas=%d\n", inv+0, dup+0, perd+0
      printf "linhas de erro no produtor=%d\n", erros
    }' | tee "$saida"
rm -f "$RES/.produtor.log"
log "salvo em $saida"
