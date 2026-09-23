# Atividade 01 

---

## Arquitetura

1. **Clientes (Produtores):** Leem imagens de pastas locais (`cliente1` e `cliente2`) e postam na fila `fila_imagens_brutas`.
2. **Conversores (Workers):** Consomem da fila de trabalho (`Work Queue`) com `prefetch_count=1`, convertem a imagem para escala de cinza e publicam na Exchange Fanout `imagens_convertidas`.
3. **Servidores de Armazenamento (Storages):** Conectados à Exchange Fanout, garantem que **todos os servidores armazenem 100% das imagens** com seus nomes originais (redundância total).

---

## Como Executar

### 1. Pré-requisitos
- Docker e Docker Compose instalados.
- Imagens de teste colocadas nas pastas:
  - `imagens_originais/cliente1/`
  - `imagens_originais/cliente2/`

### 2. Iniciar a aplicação
Na pasta `Atividade01`, execute:
```bash
docker compose up --build
```

### 3. Encerrar a aplicação
```bash
docker compose down
```

---

## 🔍 Onde Verificar os Resultados

- **Arquivos Convertidos:**
  - `armazenamento/storage1/` (todas as imagens em tons de cinza)
  - `armazenamento/storage2/` (todas as imagens em tons de cinza)
- **Painel RabbitMQ (Web):**  
  Acesse [http://localhost:15672](http://localhost:15672) (Usuário: `guest` | Senha: `guest`) para visualizar filas, conexões e exchanges em tempo real.
