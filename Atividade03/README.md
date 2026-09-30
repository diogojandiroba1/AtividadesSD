# Atividade 01 / Unidade 1 (Pub/Sub e Microsserviços)  

> **Protocolo:** MQTT (Eclipse Mosquitto) | **Linguagem:** Python 3.11 | **Conteinerização:** Docker & Docker Compose  

---

## 🏛️ 1. Arquitetura do Sistema


```
                      +-----------------------------+
                      |   Broker MQTT (Mosquitto)   |
                      |         Porta 1883          |
                      +--------------+--------------+
                                     |
         +---------------------------+---------------------------+
         |                           |                           |
+--------v-------+          +--------v-------+          +--------v-------+
|  Condutores    |          |   Veículos     |          |    Multas      |
|  Service       |          |   e IPVA       |          |    Service     |
| [condutores.db]|          | [veiculos.db]  |          |  [multas.db]   |
+----------------+          +----------------+          +--------+-------+
                                     ^                           |
                                     +--- (RPC Interno MQTT) ----+
                                     |                           |
                                     +--- (RPC Interno MQTT) ----+
```

---

## 📋 2. Requisitos Atendidos

| # | Funcionalidade | Serviço Responsável | Tópico MQTT |
|---|---|---|---|
| 1 | Emplacar veículo | `veiculos_service` | `denatran/veiculo/emplacar/req` |
| 2 | Calcular IPVA (2%) | `veiculos_service` | `denatran/veiculo/ipva/req` |
| 3 | Transferir proprietário | `veiculos_service` | `denatran/veiculo/transferir/req` |
| 4 | Cadastrar condutor | `condutores_service` | `denatran/condutor/cadastrar/req` |
| 5 | Lançar multa | `multas_service` | `denatran/multa/lancar/req` |
| 6 | Veículos emplacados em um ano | `veiculos_service` | `denatran/veiculo/por_ano/req` |
| 7 | Multas por veículo (com dados do condutor) | `multas_service` | `denatran/multa/por_veiculo_ano/req` |
| 8 | Multas de um condutor no ano | `multas_service` | `denatran/multa/por_condutor_ano/req` |
| 9 | Multas lançadas em um ano | `multas_service` | `denatran/multa/por_ano/req` |
| 10 | TOP 5 maiores pontuações | `multas_service` | `denatran/multa/top5/req` |

---

## 3. Comandos para Rodar a Aplicação

### 3.1 Via Docker & Docker Compose 

> **Pré-requisito:** Abrir **Docker Desktop** 

1. **Construir as imagens e iniciar os microsserviços em segundo plano:**
   ```bash
   docker compose up --build -d
   ```

2. **Verificar o status dos contêineres:**
   ```bash
   docker compose ps
   ```

3. **Executar Testes Automatizados (teste dos 10 requisitos):**
   ```bash
   docker compose run --rm servico_veiculos python3 tests/test_suite_completa.py
   ```

4. **Executar Menu Interativo:**
   ```bash
   docker compose run --rm servico_veiculos python3 client/cli.py
   ```

6. **Encerrar a aplicação:**
   ```bash
   docker compose down
   ```

---

## 🔍 4. Monitoramento do Barramento MQTT (Modo Raio-X)

Para auditar as mensagens trafegadas pelo barramento em tempo real durante a avaliação:

```bash
docker exec -it denatran_broker mosquitto_sub -t "denatran/#" -v
```
Isso imprimirá na tela cada requisição e resposta JSON enviada entre os serviços e o cliente.
