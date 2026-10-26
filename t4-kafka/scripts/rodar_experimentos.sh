#!/usr/bin/env bash
# Roda a bateria completa E1 a E9 (5 repeticoes nas medicoes) e guarda tudo numa pasta propria:
#   resultados/relatorio_AAAAMMDD_HHMM/  (um arquivo por experimento + execucao.log)
#   bash scripts/rodar_experimentos.sh [repeticoes=5]
# Leva por volta de 45 minutos. O cluster precisa estar no ar (bash scripts/iniciar.sh).
source "$(dirname "$0")/lib.sh"
reps=${1:-5}
export RES="$RAIZ/resultados/relatorio_$(date +%Y%m%d_%H%M)"
mkdir -p "$RES"
S="$RAIZ/scripts"
inicio=$(date +%s)

etapa() { echo; echo "################ $* ($(date +%H:%M:%S))"; }
{
  etapa "Ambiente";  uname -a; docker version --format 'Docker {{.Server.Version}}'; docker compose version
  docker stats --no-stream --format '{{.Name}}  {{.MemUsage}}  {{.CPUPerc}}'

  etapa "E1 subida e topologia";       bash "$S/status.sh" pedidos
  etapa "E2 caminho: chave -> particao"; bash "$S/ordem_chave.sh" chaves 30
  etapa "E3 replicacao";               bash "$S/e3_replicacao.sh" "$reps" 1000
  etapa "E3 produtor acks=1";          bash "$S/medir_produtor.sh" 1 "$reps"
  etapa "E3 produtor acks=all";        bash "$S/medir_produtor.sh" all "$reps"
  etapa "E4 ordem sem chave";          bash "$S/ordem_chave.sh" sem-chave 30
  etapa "E4 ordem round-robin";        bash "$S/ordem_chave.sh" round-robin 30
  etapa "E4 ordem mesma chave";        bash "$S/ordem_chave.sh" mesma-chave 30
  etapa "E5 eleicao do lider de particao"; bash "$S/medir_eleicao.sh" particao "$reps"
  etapa "E5 quorum";                   bash "$S/e5_quorum.sh"
  etapa "E6 particao de rede";         bash "$S/e6_particao.sh"
  etapa "E6/E7 recuperacao";           bash "$S/medir_recuperacao.sh" kafka3 "$reps"
  etapa "E7 grupo de consumidores";    bash "$S/e7_grupo.sh"
  etapa "E8 eleicao do controlador";   bash "$S/medir_eleicao.sh" controlador "$reps" parar
  etapa "E8 epocas";                   bash "$S/e8_epocas.sh"
  for r in $(seq 1 "$reps"); do
    etapa "E9 retries, rodada $r/$reps"; bash "$S/e9_retries.sh" todos 100000
  done
  etapa "E9 vazao 1/3/6 particoes";    bash "$S/e9_vazao.sh" "$reps" 50000
  etapa "Ambiente no fim"
  docker stats --no-stream --format '{{.Name}}  {{.MemUsage}}  {{.CPUPerc}}'
  echo "duracao total: $(( ($(date +%s) - inicio) / 60 )) min"
  echo "FIM"
} 2>&1 | tee "$RES/execucao.log"
