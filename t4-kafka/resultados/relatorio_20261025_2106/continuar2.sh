#!/usr/bin/env bash
# Segunda continuacao (25/10/2026, ~21:48): a repeticao 4 da recuperacao travou (kafka3 desconectado
# seguiu no ISR por minutos). Ficam valendo as repeticoes validas; segue com E8 e E9.
cd "$(dirname "$0")/../.."
export RES="$PWD/resultados/relatorio_20261025_2106"
S="$PWD/scripts"
etapa() { echo; echo "################ $* ($(date +%H:%M:%S))"; }
{
  etapa "Reconexao do kafka3"
  docker network connect --ip 172.30.0.43 t2-kafka_interna kafka3 2>&1
  source "$S/lib.sh"; espera_isr_completo pedidos; bash "$S/status.sh" pedidos
  etapa "E8 eleicao do controlador";   bash "$S/medir_eleicao.sh" controlador 5 parar
  etapa "E8 epocas";                   bash "$S/e8_epocas.sh"
  for r in 1 2 3 4 5; do etapa "E9 retries, rodada $r/5"; bash "$S/e9_retries.sh" todos 100000; done
  etapa "E9 vazao 1/3/6 particoes";    bash "$S/e9_vazao.sh" 5 50000
  etapa "Ambiente no fim"; docker stats --no-stream --format '{{.Name}}  {{.MemUsage}}  {{.CPUPerc}}'
  echo FIM
} 2>&1 | tee "$RES/execucao_parte3.log"
