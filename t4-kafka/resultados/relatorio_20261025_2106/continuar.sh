#!/usr/bin/env bash
# Continuacao da bateria (25/10/2026, ~21:40) depois de reiniciar os 3 nos. Ver descartados/LEIA.txt.
cd "$(dirname "$0")/../.."
export RES="$PWD/resultados/relatorio_20261025_2106"
S="$PWD/scripts"
etapa() { echo; echo "################ $* ($(date +%H:%M:%S))"; }
{
  etapa "Reinicio dos nos"; docker compose restart kafka1 kafka2 kafka3; bash "$S/iniciar.sh"
  docker stats --no-stream --format '{{.Name}}  {{.MemUsage}}  {{.CPUPerc}}'
  etapa "E6/E7 recuperacao (script corrigido)"; bash "$S/medir_recuperacao.sh" kafka3 5
  etapa "E8 eleicao do controlador";   bash "$S/medir_eleicao.sh" controlador 5 parar
  etapa "E8 epocas";                   bash "$S/e8_epocas.sh"
  for r in 1 2 3 4 5; do etapa "E9 retries, rodada $r/5"; bash "$S/e9_retries.sh" todos 100000; done
  etapa "E9 vazao 1/3/6 particoes";    bash "$S/e9_vazao.sh" 5 50000
  etapa "Ambiente no fim"; docker stats --no-stream --format '{{.Name}}  {{.MemUsage}}  {{.CPUPerc}}'
  echo FIM
} 2>&1 | tee "$RES/execucao_parte2.log"
