"""
Microsserviço: Multas e Infrações
Responsabilidade: Lançar multas, consultas por veículo/condutor/ano e ranking TOP 5.
Padrão Distribuído: Realiza chamadas RPC internas via MQTT para outros microsserviços.
"""
import sqlite3
import os
import sys
import uuid
import threading

# Garante que a pasta raiz esteja no path para importar common
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.mqtt_helper import MQTTServiceBase

DB_PATH = os.environ.get("DB_PATH", "multas.db")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS multas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ano INTEGER NOT NULL,
            descricao TEXT NOT NULL,
            pontuacao INTEGER NOT NULL,
            placa_veiculo TEXT NOT NULL,
            cpf_condutor TEXT NOT NULL,
            data_lancamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


class MultasService(MQTTServiceBase):
    def __init__(self, broker_host="mosquitto"):
        super().__init__("ServicoMultas", broker_host=broker_host)
        init_db()

        # Dicionário de controle para chamadas RPC internas
        self.reply_pendentes = {}

        # Canal exclusivo deste serviço para escutar respostas de outros microsserviços
        self.canal_respostas_internas = f"denatran/multas/respostas_internas/{uuid.uuid4().hex[:6]}"
        self.registrar_handler(self.canal_respostas_internas, self._on_resposta_interna)

        # handlers públicos de multas:
        self.registrar_handler("denatran/multa/lancar/req", self.lancar_multa)
        self.registrar_handler("denatran/multa/por_veiculo_ano/req", self.multas_veiculo_ano)
        self.registrar_handler("denatran/multa/por_condutor_ano/req", self.multas_condutor_ano)
        self.registrar_handler("denatran/multa/por_ano/req", self.multas_por_ano)
        self.registrar_handler("denatran/multa/top5/req", self.top5_pontuacoes)

    def _on_resposta_interna(self, dados: dict, envelope: dict):
        """Callback acionado quando outro microsserviço responde a nossa consulta interna."""
        corr_id = envelope.get("correlation_id")
        if corr_id in self.reply_pendentes:
            self.reply_pendentes[corr_id]["resultado"] = envelope
            self.reply_pendentes[corr_id]["evento"].set()

    def _fazer_requisicao_interna(self, topico: str, dados: dict, timeout: float = 3.0) -> dict | None:
        """
        Envia uma requisição síncrona via MQTT para outro microsserviço e aguarda a resposta.
        """
        corr_id = str(uuid.uuid4())
        evento = threading.Event()
        self.reply_pendentes[corr_id] = {"evento": evento, "resultado": None}

        envelope = {
            "correlation_id": corr_id,
            "reply_to": self.canal_respostas_internas,
            "dados": dados
        }
        self.publicar(topico, envelope)

        if evento.wait(timeout=timeout):
            res = self.reply_pendentes[corr_id]["resultado"]
            del self.reply_pendentes[corr_id]
            return res

        del self.reply_pendentes[corr_id]
        return None

    def lancar_multa(self, dados: dict, envelope: dict) -> dict:
        """
        Registra uma nova infração para um veículo.
        Entrada esperada em dados:
            {"ano": 2024, "descricao": "Excesso de velocidade", "pontuacao": 5, "placa": "ABC1D23"}
        """
        try:
            ano = int(dados.get("ano", 0))
            pontuacao = int(dados.get("pontuacao", 0))
        except (ValueError, TypeError):
            return {"status": "erro", "mensagem": "Ano e Pontuação devem ser valores numéricos válidos."}

        descricao = str(dados.get("descricao", "")).strip()
        placa = str(dados.get("placa", "")).strip().upper()

        if not placa or not descricao or pontuacao <= 0 or ano <= 0:
            return {"status": "erro", "mensagem": "Dados inválidos para lançamento de multa."}

        # Consulta o microsserviço de Veículos via MQTT para obter o CPF do proprietário no momento da infração
        resp_veiculo = self._fazer_requisicao_interna("denatran/veiculo/consultar/req", {"placa": placa})
        if not resp_veiculo or resp_veiculo.get("status") != "sucesso":
            return {"status": "erro", "mensagem": f"Veículo placa {placa} não encontrado no sistema."}

        cpf_condutor = resp_veiculo["veiculo"]["cpf_condutor"]

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO multas (ano, descricao, pontuacao, placa_veiculo, cpf_condutor) VALUES (?, ?, ?, ?, ?)",
            (ano, descricao, pontuacao, placa, cpf_condutor)
        )
        multa_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return {
            "status": "sucesso",
            "mensagem": f"Multa autuada com sucesso (ID: {multa_id}). Condutor: CPF {cpf_condutor} ({pontuacao} pontos)."
        }

    def multas_veiculo_ano(self, dados: dict, envelope: dict) -> dict:
        """
        Lista as multas cometidas por um veículo em um determinado ano (ou geral),
        exibindo os dados do condutor que levou a multa.
        """
        placa = str(dados.get("placa", "")).strip().upper()
        try:
            ano = int(dados.get("ano", 0))
        except (ValueError, TypeError):
            ano = 0

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        if ano > 0:
            cursor.execute(
                "SELECT id, ano, descricao, pontuacao, placa_veiculo, cpf_condutor FROM multas WHERE placa_veiculo = ? AND ano = ?",
                (placa, ano)
            )
        else:
            cursor.execute(
                "SELECT id, ano, descricao, pontuacao, placa_veiculo, cpf_condutor FROM multas WHERE placa_veiculo = ?",
                (placa,)
            )
        linhas = cursor.fetchall()
        conn.close()

        lista_multas = []
        for r in linhas:
            cpf = r[5]
            resp_c = self._fazer_requisicao_interna("denatran/condutor/consultar/req", {"cpf": cpf})
            nome_condutor = "Desconhecido"
            if resp_c and resp_c.get("status") == "sucesso":
                nome_condutor = resp_c.get("condutor", {}).get("nome", "Desconhecido")

            lista_multas.append({
                "id": r[0],
                "ano": r[1],
                "descricao": r[2],
                "pontuacao": r[3],
                "placa": r[4],
                "condutor": {
                    "cpf": cpf,
                    "nome": nome_condutor
                }
            })

        return {
            "status": "sucesso",
            "placa": placa,
            "total": len(lista_multas),
            "multas": lista_multas
        }

    def multas_condutor_ano(self, dados: dict, envelope: dict) -> dict:
        """
        Informa as multas recebidas por um condutor em um dado ano.
        """
        cpf = str(dados.get("cpf", "")).strip()
        try:
            ano = int(dados.get("ano", 0))
        except (ValueError, TypeError):
            ano = 0

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, ano, descricao, pontuacao, placa_veiculo FROM multas WHERE cpf_condutor = ? AND ano = ?",
            (cpf, ano)
        )
        linhas = cursor.fetchall()
        conn.close()

        multas = []
        for r in linhas:
            multas.append({
                "id": r[0],
                "ano": r[1],
                "descricao": r[2],
                "pontuacao": r[3],
                "placa": r[4]
            })

        return {
            "status": "sucesso",
            "cpf": cpf,
            "ano": ano,
            "total": len(multas),
            "multas": multas
        }

    def multas_por_ano(self, dados: dict, envelope: dict) -> dict:
        """
        Informa todas as multas lançadas em um determinado ano.
        """
        try:
            ano = int(dados.get("ano", 0))
        except (ValueError, TypeError):
            ano = 0

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, ano, descricao, pontuacao, placa_veiculo, cpf_condutor FROM multas WHERE ano = ?",
            (ano,)
        )
        linhas = cursor.fetchall()
        conn.close()

        multas = []
        for r in linhas:
            multas.append({
                "id": r[0],
                "ano": r[1],
                "descricao": r[2],
                "pontuacao": r[3],
                "placa": r[4],
                "cpf_condutor": r[5]
            })

        return {
            "status": "sucesso",
            "ano": ano,
            "total": len(multas),
            "multas": multas
        }

    def top5_pontuacoes(self, dados: dict, envelope: dict) -> dict:
        """
        Informa em ordem decrescente os 5 condutores com as maiores pontuações acumuladas de multas.
        """
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT cpf_condutor, SUM(pontuacao) as total_pontos
            FROM multas
            GROUP BY cpf_condutor
            ORDER BY total_pontos DESC
            LIMIT 5
        """)
        linhas = cursor.fetchall()
        conn.close()

        ranking = []
        for r in linhas:
            cpf = r[0]
            total_pontos = r[1]
            resp_c = self._fazer_requisicao_interna("denatran/condutor/consultar/req", {"cpf": cpf})
            nome_condutor = "Desconhecido"
            if resp_c and resp_c.get("status") == "sucesso":
                nome_condutor = resp_c.get("condutor", {}).get("nome", "Desconhecido")

            ranking.append({
                "cpf": cpf,
                "nome": nome_condutor,
                "total_pontos": total_pontos
            })

        return {
            "status": "sucesso",
            "ranking": ranking
        }


if __name__ == "__main__":
    broker = os.environ.get("BROKER_HOST", "localhost")
    service = MultasService(broker_host=broker)
    service.iniciar()
