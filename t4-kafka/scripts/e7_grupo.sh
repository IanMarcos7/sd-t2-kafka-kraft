#!/usr/bin/env bash
# E7 (parte 2): um consumidor de grupo interrompido retoma do ultimo offset confirmado,
# e as tres replicas terminam com o mesmo conteudo.
#   bash scripts/e7_grupo.sh
source "$(dirname "$0")/lib.sh"
t="e7-grupo-$(date +%H%M%S)"; g="grupo-e7-$(date +%H%M%S)"
saida="$RES/e7_grupo_$(date +%Y%m%d_%H%M%S).txt"
cria_topico "$t" 1 3; espera_isr_completo "$t"

cons() {   # le N mensagens no grupo e sai (o consumidor confirma o offset ao fechar)
  # --from-beginning so vale enquanto o grupo nao tem offset confirmado; depois ele retoma do offset
  docker exec cliente "$KBIN/kafka-console-consumer.sh" --bootstrap-server "$BS" --topic "$t" \
    --group "$g" --from-beginning --max-messages "$1" --timeout-ms 20000 2>/dev/null | tr '\n' ' '
}
{
  echo "Topico $t, grupo $g"
  echo "produzidas: $(vprod "$t" -1 30 7) mensagens (7.0 a 7.29)"
  echo
  echo "1a leitura do grupo (10 mensagens, depois o consumidor e encerrado):"
  echo "  $(cons 10)"
  echo "offsets do grupo apos a 1a leitura:"
  kt kafka-consumer-groups.sh --describe --group "$g" 2>/dev/null | sed 's/^/  /'
  echo
  echo "2a leitura do mesmo grupo (deve continuar do offset 10):"
  echo "  $(cons 10)"
  kt kafka-consumer-groups.sh --describe --group "$g" 2>/dev/null | sed 's/^/  /'
  echo
  echo "== Estado final das replicas do topico pedidos (tamanho por broker, kafka-log-dirs):"
  kt kafka-log-dirs.sh --describe --topic-list pedidos 2>/dev/null | tail -1 |
    grep -o '"broker":[0-9]*\|"partition":"[^"]*","size":[0-9]*' |
    awk -F'[:,"]+' '/broker/{b=$NF; next} {printf "  broker %s  %s  %s bytes\n", b, $3, $5}'
  echo "== Fim do log por particao (kafka-get-offsets):"
  kt kafka-get-offsets.sh --topic pedidos 2>/dev/null | sed 's/^/  /'
} 2>&1 | tee "$saida"
log "salvo em $saida"
