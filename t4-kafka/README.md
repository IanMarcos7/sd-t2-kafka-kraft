# Kit T4: Kafka (KRaft) com 3 nós, injeção de falhas e medição

Laboratório do **Trabalho 2 de Sistemas Distribuídos (UNIVALI, 2026-2), Tema 4: Mensageria publish-subscribe**, com ênfase B (Ordenação).
Ele sobe um cluster Apache Kafka 4.1 em modo KRaft com 3 nós e traz scripts para os experimentos E1 a E9 do roteiro.
Tudo roda em contêineres num computador comum (cerca de 700 MB de RAM).

## Requisitos

- Docker com Compose v2: Docker Desktop com WSL 2 no Windows, Docker Engine no Linux, Docker Desktop ou Colima no macOS
- Bash para os scripts: Git Bash no Windows (vem com o Git), o terminal normal no Linux e no macOS
- Cerca de 1 GB livre para as imagens

## Topologia

| Contêiner | Imagem | Papel | Rede interna | Rede de clientes | Limite de memória |
|---|---|---|---|---|---|
| kafka1 | apache/kafka-native:4.1.0 | broker + controlador, node.id 1 | 172.30.0.41 | 172.31.0.41:19092 | 320 MB |
| kafka2 | apache/kafka-native:4.1.0 | broker + controlador, node.id 2 | 172.30.0.42 | 172.31.0.42:19092 | 320 MB |
| kafka3 | apache/kafka-native:4.1.0 | broker + controlador, node.id 3 | 172.30.0.43 | 172.31.0.43:19092 | 320 MB |
| cliente | apache/kafka:4.1.0 | ferramentas Java (kafka-topics, perf-test, console) | — | 172.31.0.100 | 512 MB |
| kcat | edenhill/kcat:1.7.1 | consultas rápidas de metadados e consumo | — | 172.31.0.101 | 64 MB |

- **Rede interna** (`t2-kafka_interna`, 172.30.0.0/24): replicação entre brokers (porta 9092) e quórum Raft dos controladores (porta 9093).
- **Rede de clientes** (`t2-kafka_clientes`, 172.31.0.0/24): produtores e consumidores (porta 19092). Separar as redes permite isolar um nó do cluster e mantê-lo visível ao cliente (E6).
- Configuração: `min.insync.replicas=2`, fator de replicação 3 nos tópicos internos (`__consumer_offsets` e transações), `CLUSTER_ID` igual nos 3 nós, heap de 160 MB nos brokers e 256 MB no cliente.

## Subir do zero

```bash
git clone <url-do-repositorio> && cd t4-kafka
bash scripts/iniciar.sh
```

O `iniciar.sh` sobe os contêineres, espera o quórum KRaft com 3 votantes e cria o tópico `pedidos` (3 partições, fator 3). No fim, mostra o status.

No Windows, rode os comandos no **Git Bash**, não no PowerShell. Pelo PowerShell também dá: `& "C:\Program Files\Git\bin\bash.exe" scripts/iniciar.sh`.

**Entre experimentos, use `docker compose stop` / `docker compose start`.** `docker compose down` apaga os logs do Kafka. Para recomeçar do zero de propósito, use `docker compose down` seguido de `bash scripts/iniciar.sh`.

## Scripts

Todos gravam a evidência em `resultados/` (`.txt` ou `.csv` com data e hora no nome). As medições repetem 5 vezes por padrão e imprimem média e desvio padrão (R7).

| Script | Experimento | O que faz |
|---|---|---|
| `iniciar.sh` | E1 | Sobe o cluster, espera o quórum e cria `pedidos` |
| `status.sh [topico]` | E1 | Contêineres, `describe --status` do quórum e `--describe` dos tópicos |
| `falha.sh <acao> <no>` | E5–E8 | `parar`, `matar`, `iniciar`, `pausar`, `retomar`, `isolar`, `reconectar`, `lider <topico> <part>`, `controlador` |
| `ordem_chave.sh <modo> [N]` | E2, E4, E9 | Produz 1..N e mostra partição e offset de cada mensagem; modos `sem-chave`, `round-robin`, `mesma-chave`, `chaves` |
| `medir_produtor.sh [acks] [reps]` | E3 | Vazão e latência do produtor (perf-test, 50 mil mensagens de 200 bytes) |
| `medir_eleicao.sh particao [reps]` | E5 | Derruba o líder de uma partição e mede o tempo até a nova liderança |
| `medir_eleicao.sh controlador [reps] [parar\|matar]` | E8 | Derruba o controlador ativo e mede o tempo até outro assumir, com a época antes e depois (tempo lido do log Raft do novo líder) |
| `medir_recuperacao.sh [no] [reps]` | E6, E7 | Isola o nó, mede a saída do ISR, produz, reconecta e mede a volta ao ISR |
| `e9_retries.sh <perfil> [N] [falha]` | E9 | Derruba o líder no meio da produção e conta inversões, duplicatas e perdas |
| `e9_vazao.sh [reps]` | E9 | Vazão com 1, 3 e 6 partições: o custo da ordem total |

### Exemplos por experimento

```bash
# E1 - topologia
bash scripts/status.sh pedidos

# E2 - mesma chave vai sempre para a mesma particao
bash scripts/ordem_chave.sh chaves 30

# E3 - replicacao: a particao existe nos 3 nos; acks=1 x acks=all
for n in kafka1 kafka2 kafka3; do docker exec $n ls /tmp/kafka-logs | grep pedidos; done
bash scripts/medir_produtor.sh 1
bash scripts/medir_produtor.sh all

# E4 - ordem global x por particao x por chave
bash scripts/ordem_chave.sh round-robin 30
bash scripts/ordem_chave.sh mesma-chave 30

# E5 - falha do lider e perda do quorum
bash scripts/medir_eleicao.sh particao
bash scripts/falha.sh parar kafka2; bash scripts/falha.sh parar kafka3
bash scripts/medir_produtor.sh all 1     # espera NOT_ENOUGH_REPLICAS
bash scripts/medir_produtor.sh 1 1       # com acks=1 funciona
bash scripts/falha.sh iniciar kafka2; bash scripts/falha.sh iniciar kafka3

# E6 / E7 - particao de rede e recuperacao
bash scripts/medir_recuperacao.sh kafka3

# E8 - eleicao do controlador e epoca
bash scripts/medir_eleicao.sh controlador

# E9 - enfase B (ordenacao)
bash scripts/ordem_chave.sh sem-chave 30
bash scripts/e9_retries.sh todos
bash scripts/e9_vazao.sh
```

### Perfis do `e9_retries.sh`

Em todos os perfis o produtor usa `acks=all`. O script envia os números 1..N com a mesma chave numa partição e derruba o líder quando cerca de 1/4 das mensagens já chegou.

| Perfil | Configuração | O que a teoria prevê |
|---|---|---|
| `sem-idemp-5` | `enable.idempotence=false`, `max.in.flight.requests.per.connection=5`, retries no máximo | Pode inverter a ordem (um lote reenviado passa à frente de outro) e duplicar |
| `sem-idemp-1` | `enable.idempotence=false`, `max.in.flight=1` | Ordem mantida, ainda pode duplicar |
| `idemp-5` | `enable.idempotence=true`, `max.in.flight=5` | Ordem mantida e sem duplicatas: o broker usa número de sequência por produtor |
| `sem-retries` | `enable.idempotence=false`, `retries=0` | Pode perder mensagens |

A falha pode ser `matar` (padrão), `parar`, `isolar` ou `pausar`. Inversões dependem de um lote falhar enquanto o seguinte é aceito, então podem não aparecer em toda rodada. Repita algumas vezes e registre quantas rodadas mostraram cada efeito.

### Observações de medição

- Os tempos de eleição de partição e de recuperação usam o `kcat`, que consulta os metadados em dezenas de milissegundos. A ferramenta Java leva vários segundos por chamada, então o tempo do controlador (E8) vem do horário registrado no log do novo líder Raft ("Completed transition to Leader"). A coluna `tempo_deteccao_ms` mostra quando o script percebeu a troca, só como referência.
- Isolado da rede interna, o nó também perde contato com os controladores e é afastado quando expira a sessão do broker (`broker.session.timeout.ms`, 9 s). No teste, ele saiu do ISR em cerca de 10 s. Para um seguidor que só fica para trás, o limite seria `replica.lag.time.max.ms` (30 s). Cada repetição do `medir_recuperacao.sh` leva por volta de 1 minuto.
- A partição de uma chave é `murmur2(chave) % número de partições`. Com 3 partições, `cliente1`, `cliente2` e `cliente3` caem **todas na P0**. Por isso o modo `chaves` usa `cliente1`, `cliente4` e `cliente10`, que caem em P0, P1 e P2. Chave diferente não garante partição diferente.
- O particionador padrão sem chave é "sticky": ele manda cada lote inteiro para uma partição. Com poucas mensagens, é comum tudo cair numa partição só. O modo `round-robin` espalha as mensagens uma a uma.

## Armadilhas (seção 8.6 do roteiro)

- Os advertised listeners anunciam o IP de cada rede; se estiverem errados, o cliente não alcança os outros nós.
- O heap do cliente está limitado em 256 MB. Não rode várias ferramentas Java ao mesmo tempo.
- O Kafka 4.x mostra `Elr` (eligible leader replicas, KIP-966) no `--describe`.
- Com `acks=all` e ISR abaixo do mínimo, o produtor de console mostra `NotEnoughReplicasException` mas pode não terminar com erro. Leia a saída.
- `CLUSTER_ID` diferente entre os nós impede a formação do quórum.

## Estrutura

```
t4-kafka/
├── docker-compose.yml
├── README.md
├── scripts/        # lib.sh + scripts dos experimentos
└── resultados/     # saídas geradas (evidências do relatório)
```
