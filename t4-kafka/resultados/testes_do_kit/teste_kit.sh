#!/usr/bin/env bash
cd "$(dirname "$0")/../.."
export RES="$PWD/resultados/testes_do_kit/novos2"; mkdir -p "$RES"
for c in "e6_particao.sh" "e7_grupo.sh" "e8_epocas.sh"; do
  echo "### $c"; bash scripts/$c
done > "$RES/teste.log" 2>&1
echo FIM >> "$RES/teste.log"
