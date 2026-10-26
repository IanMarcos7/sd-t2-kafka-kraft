#!/usr/bin/env bash
# Terceira continuacao (25/10/2026, ~22:10): a vazao caiu de 12 mil para 419 msg/s entre as rodadas
# porque os nos chegaram perto do limite de memoria (muitos topicos de teste criados ao longo de 3 h).
# Apaga os topicos de teste, reinicia os nos e refaz so a medicao de vazao do E9.
cd "$(dirname "$0")/../.."
export RES="$PWD/resultados/relatorio_20261025_2106"
S="$PWD/scripts"; source "$S/lib.sh"
etapa() { echo; echo "################ $* ($(date +%H:%M:%S))"; }
{
  etapa "Limpeza"; docker stats --no-stream --format '{{.Name}}  {{.MemUsage}}  {{.CPUPerc}}'
  for p in 'e9-.*' 'ordem-.*' 'e5-.*' 'e6-.*' 'e7-.*' 'e3-.*' 'dbg-.*' 'vazao-.*'; do
    kt kafka-topics.sh --delete --topic "$p" 2>&1 | tail -1
  done
  kt kafka-topics.sh --list --exclude-internal
  docker compose restart kafka1 kafka2 kafka3; bash "$S/iniciar.sh" >/dev/null
  sleep 20; docker stats --no-stream --format '{{.Name}}  {{.MemUsage}}  {{.CPUPerc}}'
  etapa "E9 vazao 1/3/6 particoes";    bash "$S/e9_vazao.sh" 5 50000
  etapa "Ambiente no fim"; docker stats --no-stream --format '{{.Name}}  {{.MemUsage}}  {{.CPUPerc}}'
  echo FIM
} 2>&1 | tee "$RES/execucao_parte4.log"
