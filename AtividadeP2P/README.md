# Atividade 02 - Avaliação de Desempenho: Cliente-Servidor vs P2P

Avaliação comparativa do tempo de transferência de arquivos sob diferentes arquiteturas de distribuição.

## Dependências

```bash
sudo apt update && sudo apt install -y aria2 mktorrent python3
```

## 1. Preparação dos Dados

Geração dos arquivos de teste (5MB, 50MB e 500MB):
```bash
python3 gerar_arq.py
```

## 2. Testes Cliente-Servidor

Iniciar o servidor escolhendo o modelo de concorrência:

```bash
# Iterativo (1 conexão por vez)
python3 servidor.py iterativo <caminho_arquivo>

# Uma thread por cliente
python3 servidor.py thread <caminho_arquivo>

# Pool de threads (limite N)
python3 servidor.py pool <caminho_arquivo> <limite_N>
```

Em outro terminal, executar o cliente de benchmark:
```bash
python3 benchmark.py cs <num_clientes> [ip_servidor] [porta]  # Deixar sem ip e porta, caso não estiver rodando nada nas portas.
```

## 3. Testes P2P (BitTorrent via aria2c)

1. **Gerar o arquivo `.torrent`:**
   ```bash
   mktorrent -a [http://127.0.0.1:6969/announce](http://127.0.0.1:6969/announce) <caminho_arquivo>
   ```

2. **Terminal 1 — Iniciar o Tracker:**
   ```bash
   python3 tracker.py
   ```

3. **Terminal 2 — Iniciar o Seeder Inicial:**
   ```bash
   aria2c --enable-dht=false --bt-seed-unverified=true --seed-ratio=0.0 <arquivo.torrent>
   ```

4. **Terminal 3 — Executar o Benchmark P2P:**
   ```bash
   python3 benchmark.py p2p <num_clientes> <arquivo.torrent>
   ```

## 4. Métricas Coletadas

Para cada combinação de arquivo e quantidade de clientes, o benchmark calcula:
* **Tempo Mínimo** de conclusão
* **Tempo Médio**
* **Tempo Máximo** (tempo total para todos os nós finalizarem)