"""
Microsserviço: Condutores
Cadastrar e consultar condutores pelo CPF.
"""
import sqlite3
import os
import sys

# Garante que a pasta raiz esteja no path para importar common
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.mqtt_helper import MQTTServiceBase

DB_PATH = os.environ.get("DB_PATH", "condutores.db")

def init_db():
    """Cria a tabela condutores se não existir."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS condutores (
            cpf TEXT PRIMARY KEY,
            nome TEXT NOT NULL,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()  
    conn.close()

class CondutoresService(MQTTServiceBase):
    def __init__(self, broker_host="mosquitto"):
        super().__init__("ServicoCondutores", broker_host=broker_host)
        init_db()
        # registra os tópicos que esse serviço escuta
        self.registrar_handler("denatran/condutor/cadastrar/req", self.cadastrar_condutor)
        self.registrar_handler("denatran/condutor/consultar/req", self.consultar_condutor)

    def cadastrar_condutor(self, dados: dict, envelope: dict) -> dict:
        """Processa a requisição de cadastro de condutor."""
        cpf = str(dados.get("cpf", "")).strip()
        nome = str(dados.get("nome", "")).strip()

        if not cpf or not nome:
            return {"status": "erro", "mensagem": "CPF e Nome são obrigatórios."}

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO condutores (cpf, nome) VALUES (?, ?)", (cpf, nome))
            conn.commit()
            return {"status": "sucesso", "mensagem": f"Condutor {nome} cadastrado com sucesso!"}
        except sqlite3.IntegrityError:
            return {"status": "erro", "mensagem": f"Condutor com CPF {cpf} já está cadastrado."}
        finally:
            conn.close()

    def consultar_condutor(self, dados: dict, envelope: dict) -> dict:
        """Processa a requisição de consulta por CPF."""
        cpf = str(dados.get("cpf", "")).strip()  
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT cpf, nome FROM condutores WHERE cpf = ?", (cpf,))
        resultado = cursor.fetchone() 
        conn.close()
        
        if resultado is not None:

            return {
                "status": "sucesso",
                "condutor": {
                    "cpf": resultado[0],
                    "nome": resultado[1]
                }
            }
        else:
            return {
                "status": "erro",
                "mensagem": f"Condutor com CPF {cpf} não encontrado."
            }

if __name__ == "__main__":
    broker = os.environ.get("BROKER_HOST", "localhost")
    service = CondutoresService(broker_host=broker)
    service.iniciar()
