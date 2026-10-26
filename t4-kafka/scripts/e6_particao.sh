#!/usr/bin/env bash
# E6: particao de rede. Tira o lider de uma particao da rede interna (ele continua na rede de
# clientes) e observa os dois lados: o que o no isolado aceita, o que a maioria faz, e o que
# sobra depois da reconexao.
#   bash scripts/e6_particao.sh
# Prefixos das mensagens: 0=antes, 1=isolado acks=all, 2=isolado acks=1, 3=maioria acks=all
# O produtor recebe a lista de brokers pela rede de clientes; quando o no isolado para de responder,
# ele pode descobrir o novo lider e escrever nele. Os horarios de confirmacao (JSON) mostram onde
# cada lote foi parar, e o conteudo final do log mostra o que sobreviveu.
source "$(dirname "$0")/lib.sh"
t="e6-particao-$(date +%H%M%S)"
base="$RES/e6_particao_$(date +%Y%m%d_%H%M%S)"; saida="$base.txt"
export VPROD_LOG="$base.produtor.jsonl"; : >"$VPROD_LOG"
cria_topico "$t" 1 3; espera_isr_completo "$t"
L=$(lider "$t" 0); alvo=kafka$L
bs_iso="172.31.0.4$L:19092"; bs_maj=$(bs_sem "$alvo")

conf() {  # resumo das confirmacoes de um prefixo: quantas, offsets e segundos apos o isolamento
  grep '"producer_send_success"' "$VPROD_LOG" | grep "\"value\":\"$1\." |
    awk -v t0="$t0" -F'[:,}]' '{for(i=1;i<=NF;i++){if($i ~ /"timestamp"/) ts=$(i+1); if($i ~ /"offset"/) of=$(i+1)}
       n++; if(n==1){a=ts; o1=of} b=ts; o2=of}
       END{ if(n) printf "%d confirmadas, offsets %s-%s, entre %.1f e %.1f s apos o isolamento", n, o1, o2, (a-t0)/1000, (b-t0)/1000;
            else printf "0 confirmadas" }'
}
{
  echo "Topico $t (1 particao, fator 3). Lider antes da particao: $alvo"
  echo "antes: $(vprod "$t" -1 10 0) de 10 escritas com acks=all"
  echo
  log "isolando $alvo da rede interna"
  t0=$(ms); docker network disconnect "$REDE_INTERNA" "$alvo"
  echo "== Lado minoritario (cliente com bootstrap so em $alvo)"
  echo "  metadados vistos por $alvo: lider=$(lider_via "$t" 0 "$bs_iso")"
  vprod "$t" -1 10 1 "$bs_iso" >/dev/null; echo "  escrita acks=all: $(conf 1)"
  vprod "$t" 1 10 2 "$bs_iso" >/dev/null;  echo "  escrita acks=1:   $(conf 2)"
  echo
  echo "== Lado majoritario"
  novo=""
  for _ in $(seq 1 120); do
    v=$(lider_via "$t" 0 "$bs_maj"); novo=${v%% *}
    [ -n "$novo" ] && [ "$novo" != "$L" ] && [ "$novo" != "-1" ] && break
    sleep 0.5
  done
  echo "  novo lider eleito pela maioria: kafka$novo, visto $(( ($(ms) - t0) / 1000 )) s apos o isolamento; $(lider_via "$t" 0 "$bs_maj")"
  VPROD_TIMEOUT=30000 vprod "$t" -1 10 3 "$bs_maj" >/dev/null
  echo "  escrita acks=all na maioria: $(conf 3)"
  echo "  metadados vistos por $alvo agora: lider=$(lider_via "$t" 0 "$bs_iso")"
  echo "  log de $alvo (ISR que ele tentou mudar / perda de contato):"
  docker logs --since 3m "$alvo" 2>&1 | grep "$t" | grep -i -E "shrink|isr|fenc" | tail -2 | cut -c1-200 | sed 's/^/    /'
  echo
  log "reconectando $alvo"
  docker network connect --ip "$(ip_interno "$alvo")" "$REDE_INTERNA" "$alvo"
  # espera o ISR completo visto pela maioria (o no que volta pode responder com metadados antigos)
  for _ in $(seq 1 120); do
    v=$(lider_via "$t" 0 "$bs_maj"); [ "$(tr ',' '\n' <<<"${v##*isr=}" | wc -l)" -ge 3 ] && break; sleep 0.5
  done
  echo "== Depois de reconectar (ISR: $(isr "$t" 0), lider: kafka$(lider "$t" 0))"
  echo "  mensagens que sobreviveram, por origem (lidas ate o high watermark):"
  echo "    antes da particao (0):     $(vconta "$t" 0 0) de 10"
  echo "    isolado, acks=all (1):     $(vconta "$t" 0 1) de 10"
  echo "    isolado, acks=1 (2):       $(vconta "$t" 0 2) de 10"
  echo "    maioria, acks=all (3):     $(vconta "$t" 0 3) de 10"
  echo "  conteudo final do log (offset:valor):"
  timeout 25 docker exec kcat kcat -b "$BS" -C -t "$t" -p 0 -o beginning -e -q -f '%o:%s ' 2>/dev/null | fold -w 110 | sed 's/^/    /'; echo
  echo "  log de $alvo sobre truncamento ao voltar:"
  docker logs --since 5m "$alvo" 2>&1 | grep -i "truncat" | grep "$t" | tail -2 | cut -c1-200 | sed 's/^/    /'
} 2>&1 | tee "$saida"
log "salvo em $saida (confirmacoes do produtor em $VPROD_LOG)"
