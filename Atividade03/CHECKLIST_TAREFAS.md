# Checklist Detalhado de Tarefas — Atividade 03 (DENATRAN / MQTT)

> **Disciplina:** Sistemas Distribuídos — Universidade Federal de Sergipe (UFS)  
> **Professor:** Rafael Oliveira Vasconcelos  
> **Foco:** Arquitetura de Microsserviços, Protocolo MQTT (Pub/Sub e Request-Reply), Isolamento de Dados (Database per Service) e Conteinerização (Docker).

---

## 📌 Legenda de Status
- `[x]` **Concluído:** Arquivo ou funcionalidade já criada e pronta para uso.
- `[ ]` **Pendente para Você Implementar:** Código com marcações `# PASSO X` onde você deve escrever a lógica.
- `[SD]` **Conceito Chave de Sistemas Distribuídos:** Destaque para pontos teóricos avaliados na disciplina.

---

## 1. Módulo Compartilhado: `common/mqtt_helper.py`
*Camada de abstração do barramento de comunicação MQTT (Request-Reply sobre Pub/Sub).*

- [x] `[SD]` **Inicialização do Cliente MQTT:** Instanciação do `paho.mqtt.client.Client` com a API moderna `CallbackAPIVersion.VERSION2`.
- [x] `[SD]` **Roteador Interno de Tópicos:** Implementação do método `registrar_handler(topico, func)` para mapear mensagens a callbacks.
- [x] `[SD]` **Inscrição Automática (Sub):** Implementação de `_on_connect` que subscreve em todos os tópicos registrados com garantia de entrega **QoS 1**.
- [x] `[SD]` **Despacho e Envelope Request-Reply:** No `_on_message`:
  - Deserialização de JSON UTF-8 com tratamento de erros.
  - Extração de `correlation_id` e `reply_to`.
  - Execução do handler de negócio.
  - Empacotamento da resposta com o mesmo `correlation_id` e publicação de volta no tópico especificado em `reply_to`.
- [x] `[SD]` **Tratamento de Exceções Distribuídas:** Captura de falhas no processamento interno e publicação automática de mensagem de erro estruturada `{"status": "erro", "mensagem": ...}` para o cliente requisitante não travar.
- [x] `[SD]` **Serialização e Envio:** Método `publicar(topico, dados_dict, qos=1)` convertendo dicionários Python para bytes JSON.
- [x] **Loop de Rede:** Método `iniciar()` executando `client.loop_forever()` para manter o microsserviço ativo.

---

## 2. Microsserviço de Condutores: `services/condutores_service.py`
*Responsável exclusivo pelo ciclo de vida dos motoristas habilitados.*

- [x] `[SD]` **Isolamento de Dados (Database per Service):** Configuração do banco SQLite exclusivo `condutores.db`.
- [x] **Inicialização do Banco (`init_db`):** Criação da tabela `condutores` (`cpf TEXT PRIMARY KEY, nome TEXT NOT NULL, criado_em TIMESTAMP`).
- [x] `[SD]` **Herança e Registro no Barramento:** Classe herdando de `MQTTServiceBase` e registrando os tópicos:
  - `denatran/condutor/cadastrar/req`
  - `denatran/condutor/consultar/req`
- [x] **Cadastrar Condutor (`cadastrar_condutor`):** Validação de CPF e Nome, inserção no SQLite e tratamento de `sqlite3.IntegrityError` (evitando duplicações).
- [x] `[SD]` **Consulta para RPC Interno (`consultar_condutor`):** Retorna os dados do motorista para atender tanto ao cliente quanto a chamadas internas de outros microsserviços.
- [x] **Entrypoint autônomo:** Leitura de `BROKER_HOST` e `DB_PATH` via variáveis de ambiente.

---

## 3. Microsserviço de Veículos e IPVA: `services/veiculos_service.py`
*Responsável pelo registro de veículos, cálculo de imposto e transferência de titularidade.*

- [x] **Esqueleto e Estrutura Base:** Imports, docstrings, type hints e chamada de inicialização.
- [x] `[SD]` **PASSO 1 — Inicialização do Banco (`init_db`):**
  - [x] Conectar ao `DB_PATH` (`veiculos.db`).
  - [x] Criar a tabela `veiculos` com as colunas:
    - `placa TEXT PRIMARY KEY`
    - `modelo TEXT NOT NULL`
    - `valor REAL NOT NULL`
    - `cpf_condutor TEXT NOT NULL`
    - `ano_emplacamento INTEGER NOT NULL`
    - `atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
  - [x] Efetuar `commit()` e fechar a conexão.
- [x] `[SD]` **PASSO 2 — Registro de Tópicos no `__init__`:**
  - [x] Descomentar e registrar os 5 handlers no MQTT:
    - `denatran/veiculo/emplacar/req` -> `self.emplacar_veiculo`
    - `denatran/veiculo/ipva/req` -> `self.calcular_ipva`
    - `denatran/veiculo/transferir/req` -> `self.transferir_proprietario`
    - `denatran/veiculo/por_ano/req` -> `self.veiculos_por_ano`
    - `denatran/veiculo/consultar/req` -> `self.consultar_veiculo`
- [x] **PASSOS 3 a 5 — Emplacar Veículo (`emplacar_veiculo`):**
  - [x] Extrair os campos: `placa` (em maiúsculas `.upper()`), `modelo`, `valor` (float), `cpf_condutor`, `ano` (int).
  - [x] Validar campos obrigatórios e garantir `valor > 0`.
  - [x] Executar `INSERT INTO veiculos` com parâmetros seguros `(?, ?, ?, ?, ?)`.
  - [x] Tratar exceção `sqlite3.IntegrityError` retornando mensagem de placa já cadastrada.
  - [x] Retornar `{"status": "sucesso", "mensagem": "..."}`.
- [x] `[SD]` **PASSOS 6 a 9 — Calcular IPVA (`calcular_ipva`):**
  - [x] Obter a `placa` em maiúsculas.
  - [x] Executar `SELECT valor, modelo FROM veiculos WHERE placa = ?`.
  - [x] Se não encontrar, retornar `status: "erro"`.
  - [x] Aplicar a regra de negócio da disciplina: `aliquota = 0.02` e `ipva = round(valor * aliquota, 2)`.
  - [x] Retornar dicionário com: `status`, `placa`, `modelo`, `valor_venal`, `aliquota: "2%"` e `valor_ipva`.
- [x] **PASSOS 10 a 12 — Transferir Proprietário (`transferir_proprietario`):**
  - [x] Extrair `placa` e `novo_cpf`.
  - [x] Verificar se o veículo existe no banco.
  - [x] Executar `UPDATE veiculos SET cpf_condutor = ?, atualizado_em = CURRENT_TIMESTAMP WHERE placa = ?`.
  - [x] Retornar confirmação da transferência.
- [x] **PASSOS 13 a 14 — Veículos Emplacados por Ano (`veiculos_por_ano`):**
  - [x] Extrair o `ano`.
  - [x] Executar `SELECT placa, modelo, valor, cpf_condutor, ano_emplacamento FROM veiculos WHERE ano_emplacamento = ?`.
  - [x] Montar a lista de dicionários e retornar `{"status": "sucesso", "ano": ano, "total": len(...), "veiculos": [...]}`.
- [x] `[SD]` **PASSOS 15 a 16 — Consulta de Veículo (`consultar_veiculo`):**
  - [x] Executar `SELECT` por `placa`.
  - [x] Retornar dados do veículo para responder a chamadas internas de outros microsserviços.

---

## 4. Microsserviço de Multas e Infrações: `services/multas_service.py`
*Ponto central de Sistemas Distribuídos: orquestração de dados e chamadas RPC inter-serviços.*

- [x] **Esqueleto e Estrutura Base:** Imports, docstrings, canal exclusivo de resposta interno.
- [x] `[SD]` **Mecanismo de RPC Interno (`_fazer_requisicao_interna` e `_on_resposta_interna`):**
  - Implementação completa do padrão síncrono com `threading.Event()` e `correlation_id` para consultar outros serviços sem travar o barramento.
- [x] `[SD]` **PASSO 1 — Inicialização do Banco (`init_db`):**
  - [x] Conectar ao `DB_PATH` (`multas.db`).
  - [x] Criar tabela `multas` com:
    - `id INTEGER PRIMARY KEY AUTOINCREMENT`
    - `ano INTEGER NOT NULL`
    - `descricao TEXT NOT NULL`
    - `pontuacao INTEGER NOT NULL`
    - `placa_veiculo TEXT NOT NULL`
    - `cpf_condutor TEXT NOT NULL` *(proprietário no momento da infração)*
    - `data_lancamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
- [x] `[SD]` **PASSO 2 — Registro de Tópicos no `__init__`:**
  - [x] Descomentar e registrar os 5 handlers públicos:
    - `denatran/multa/lancar/req` -> `self.lancar_multa`
    - `denatran/multa/por_veiculo_ano/req` -> `self.multas_veiculo_ano`
    - `denatran/multa/por_condutor_ano/req` -> `self.multas_condutor_ano`
    - `denatran/multa/por_ano/req` -> `self.multas_por_ano`
    - `denatran/multa/top5/req` -> `self.top5_pontuacoes`
- [x] `[SD]` **PASSOS 3 a 6 — Lançar Multa (`lancar_multa`):**
  - [x] Extrair e validar: `ano`, `descricao`, `pontuacao` e `placa`.
  - [x] **RPC Interno:** Enviar requisição via MQTT para descobrir o proprietário atual:
    ```python
    resp = self._fazer_requisicao_interna("denatran/veiculo/consultar/req", {"placa": placa})
    ```
  - [x] Se o veículo não existir ou a resposta falhar, retornar erro.
  - [x] Obter `cpf_condutor = resp["veiculo"]["cpf_condutor"]`.
  - [x] Inserir a multa em `multas.db` vinculada a esse CPF.
  - [x] Retornar confirmação com ID da multa, CPF autuado e pontos.
- [x] `[SD]` **PASSOS 7 a 9 — Multas por Veículo em um Ano (`multas_veiculo_ano`):**
  - [x] Consultar em `multas.db` filtrando por `placa_veiculo` (e `ano` se especificado).
  - [x] **RPC Interno:** Para cada multa retornada, consultar o nome do condutor no serviço de condutores:
    ```python
    resp_condutor = self._fazer_requisicao_interna("denatran/condutor/consultar/req", {"cpf": cpf})
    ```
  - [x] Montar a lista enriquecida com os dados do condutor (`cpf` e `nome`) e retornar.
- [x] **PASSOS 10 a 11 — Multas por Condutor em um Ano (`multas_condutor_ano`):**
  - [x] Executar `SELECT` em `multas.db` com `WHERE cpf_condutor = ? AND ano = ?`.
  - [x] Retornar a listagem de multas daquele motorista no ano.
- [x] **PASSOS 12 a 13 — Multas Lançadas no Ano (`multas_por_ano`):**
  - [x] Executar `SELECT` em `multas.db` com `WHERE ano = ?`.
  - [x] Retornar a listagem geral de infrações do ano.
- [x] `[SD]` **PASSOS 14 a 16 — TOP 5 Condutores com Maior Pontuação (`top5_pontuacoes`):**
  - [x] Executar agregação SQL em `multas.db`:
    ```sql
    SELECT cpf_condutor, SUM(pontuacao) as total 
    FROM multas 
    GROUP BY cpf_condutor 
    ORDER BY total DESC 
    LIMIT 5;
    ```
  - [x] **RPC Interno:** Para cada condutor do TOP 5, consultar o nome via `denatran/condutor/consultar/req`.
  - [x] Retornar o ranking ordenado com `cpf`, `nome` e `total_pontos`.

---

## 5. Interface Interativa do Cliente: `client/cli.py`
*Ponto de entrada para o operador do sistema DENATRAN.*

- [x] `[SD]` **Classe `DenatranClient`:** Conexão MQTT em background via `loop_start()`, canal exclusivo de respostas e correlação síncrona.
- [x] **Menu do Terminal (`imprimir_menu`):** Interface textual amigável numerada de 0 a 10.
- [x] **Captura de Entradas e Sanitização:** Leitura de placas, CPFs, anos e valores com proteção contra entradas inválidas.
- [x] **Encaminhamento de Mensagens:** Chamadas automáticas de `enviar_requisicao` para cada tópico com timeout de 5 segundos.
- [ ] **Validação Prática:** Executar `python client/cli.py` no terminal e testar interativamente as opções conforme implementar os serviços.

---

## 6. Suíte de Testes Automatizados: `tests/test_suite_completa.py`
*Homologação de todos os 10 requisitos do enunciado em fluxo contínuo.*

- [x] **Conexão com Broker:** Configuração dinâmica de host via `BROKER_HOST`.
- [x] **Teste 1:** Cadastro de 6 condutores (Senna, Hamilton, Verstappen, Barrichello, Massa, Prost).
- [x] **Teste 2:** Emplacamento de 3 veículos (McLaren, Mercedes, Red Bull).
- [x] `[SD]` **Teste 3:** Validação do cálculo de 2% do IPVA com asserção explícita (`assert ipva == 10000.0`).
- [x] **Teste 4:** Transferência do veículo `HAM2008` para Rubens Barrichello.
- [x] `[SD]` **Teste 5:** Lançamento de 5 multas, testando o vínculo automático com o dono do carro.
- [x] **Teste 6:** Consulta de veículos emplacados em 2024.
- [x] `[SD]` **Teste 7:** Consulta de multas por veículo trazendo condutor penalizado via RPC.
- [x] **Teste 8:** Consulta de multas por CPF e ano.
- [x] **Teste 9:** Consulta de total de multas lançadas no ano.
- [x] `[SD]` **Teste 10:** Consulta e validação do ranking TOP 5 condutores com nomes enriquecidos.
- [x] **Validação Prática:** Executar a suíte e obter `TODOS OS TESTES EXECUTADOS COM SUCESSO!`.

---

## 7. Infraestrutura de Mensageria e Conteinerização

### 7.1 Configuração do Broker: `mosquitto/config/mosquitto.conf`
- [x] `[SD]` Configurar `listener 1883` em todas as interfaces de rede do container.
- [x] `[SD]` Permitir conexões sem autenticação (`allow_anonymous true`).
- [x] `[SD]` Desativar persistência em disco (`persistence false`) para testes efêmeros e rápidos.

### 7.2 Dependências: `requirements.txt`
- [x] Fixar `paho-mqtt>=2.0.0`.
- [x] Manter o projeto com zero dependências externas extras (usando stdlib: `sqlite3`, `json`, `uuid`, `threading`).

### 7.3 Imagem Docker: `Dockerfile`
- [x] Imagem base `python:3.11-slim`.
- [x] Variáveis de ambiente de performance: `PYTHONDONTWRITEBYTECODE=1` e `PYTHONUNBUFFERED=1`.
- [x] Cache eficiente de dependências (`COPY requirements.txt` antes do código-fonte).

### 7.4 Orquestração Multi-Serviços: `docker-compose.yml`
- [x] `[SD]` **DNS Interno e Rede Bridge (`denatran_net`):** Comunicação entre contêineres por nome de serviço (`mosquitto`).
- [x] `[SD]` **Database per Service com Volumes Isolados:**
  - `dados_condutores` montado em `/app/data/condutores.db`.
  - `dados_veiculos` montado em `/app/data/veiculos.db`.
  - `dados_multas` montado em `/app/data/multas.db`.
- [x] `[SD]` **Ordem de Inicialização (`depends_on`):** Garantir que o Mosquitto suba antes dos microsserviços.

---

## 🎯 Roteiro Recomendado para sua Implementação

1. **Abra o arquivo [veiculos_service.py](file:///c:/Users/Diogo/Desktop/UFS_2026.2/AtividadesSD/Atividade03/services/veiculos_service.py)**:
   - Siga os passos de 1 a 16 implementando as funções de banco e lógica de IPVA/emplacamento.
2. **Abra o arquivo [multas_service.py](file:///c:/Users/Diogo/Desktop/UFS_2026.2/AtividadesSD/Atividade03/services/multas_service.py)**:
   - Siga os passos de 1 a 16 usando o método pronto `_fazer_requisicao_interna` para buscar o condutor no serviço de veículos e condutores.
3. **Execute os testes**:
   ```powershell
   python tests\test_suite_completa.py
   ```
4. **Suba com Docker Compose para validar a entrega**:
   ```bash
   docker compose up --build -d
   docker compose run --rm servico_veiculos python3 tests/test_suite_completa.py
   ```
