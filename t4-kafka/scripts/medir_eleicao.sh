#!/usr/bin/env bash
# E5 / E8: tempo de eleicao de um novo lider, repetido N vezes.
#   bash scripts/medir_eleicao.sh particao     [repeticoes=5] [topico=pedidos] [particao=0] [parar|matar]
#   bash scripts/medir_eleicao.sh controlador  [repeticoes=5] [parar|matar]
# "particao": derruba o lider da particao e mede ate o controlador escolher outro do ISR.
# "controlador": derruba o controlador ativo do quorum KRaft e mede ate surgir outro (epoca sobe).
# Resolucao: kcat consulta em ~50 ms. No modo controlador, o tempo vem do log do novo lider Raft
# ("Completed transition to Leader"); a deteccao pela ferramenta Java (~3 s por consulta) fica
# numa coluna a parte so como referencia.
source "$(dirname "$0")/lib.sh"
modo=${1:-particao}; reps=${2:-5}
if [ "$modo" = controlador ]; then como=${3:-parar}
else topico=${3:-pedidos}; part=${4:-0}; como=${5:-parar}; fi
csv="$RES/eleicao_${modo}_$(date +%Y%m%d_%H%M%S).csv"

if [ "$modo" = controlador ]; then
  echo "rep,lider_antigo,epoca_antiga,lider_novo,epoca_nova,tempo_ms,tempo_deteccao_ms" >"$csv"
  for r in $(seq 1 "$reps"); do
    read -r antigo ep_antiga < <(controlador)
    alvo=kafka$antigo
    log "rep $r/$reps: controlador ativo = $alvo (epoca $ep_antiga); derrubando"
    t0=$(ms)
    # em segundo plano: docker stop so retorna quando o conteiner termina de desligar
    if [ "$como" = matar ]; then docker kill "$alvo" >/dev/null & else docker stop "$alvo" >/dev/null & fi
    outros=$(bs_sem "$alvo")
    while :; do
      read -r novo ep_nova < <(controlador "$outros")
      [ -n "${novo:-}" ] && [ "$novo" != "$antigo" ] && [ "$novo" != "-1" ] && break
      sleep 0.2
    done
    t1=$(ms); wait
    t_log=$(ts_lider_raft "kafka$novo")
    t_ele=$(( ${t_log:-$t1} - t0 ))
    echo "$r,$antigo,$ep_antiga,$novo,$ep_nova,$t_ele,$((t1 - t0))" | tee -a "$csv"
    docker start "$alvo" >/dev/null; espera_no "$alvo"; sleep 5
  done
else
  cria_topico "$topico" 3 3
  echo "rep,topico,particao,lider_antigo,lider_novo,isr_depois,tempo_ms" >"$csv"
  for r in $(seq 1 "$reps"); do
    espera_isr_completo "$topico"
    antigo=$(lider "$topico" "$part"); alvo=kafka$antigo
    log "rep $r/$reps: lider de $topico/$part = $alvo; derrubando ($como)"
    t0=$(ms)
    if [ "$como" = matar ]; then docker kill "$alvo" >/dev/null & else docker stop "$alvo" >/dev/null & fi
    while :; do
      novo=$(lider "$topico" "$part")
      [ -n "$novo" ] && [ "$novo" != "$antigo" ] && [ "$novo" != "-1" ] && break
      sleep 0.05
    done
    t1=$(ms); wait
    echo "$r,$topico,$part,$antigo,$novo,\"$(isr "$topico" "$part")\",$((t1 - t0))" | tee -a "$csv"
    docker start "$alvo" >/dev/null; espera_no "$alvo"
  done
fi

echo
col=0; [ "$modo" = controlador ] && col=6     # 0 = ultima coluna
echo "tempo de eleicao (ms): $(tail -n +2 "$csv" | awk -F, -v c="$col" '{print (c ? $c : $NF)}' | estat)"
log "salvo em $csv"
