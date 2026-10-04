"""
Microsserviço: Veículos e IPVA
Emplacamento, cálculo de IPVA (2%), transferência de proprietário e consultas.
"""
import sqlite3
import os
import sys

# Garante que a pasta raiz esteja no path para importar common
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.mqtt_helper import MQTTServiceBase

DB_PATH = os.environ.get("DB_PATH", "veiculos.db")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS veiculos (
            placa TEXT PRIMARY KEY,
            modelo TEXT NOT NULL,
            valor REAL NOT NULL,
            cpf_condutor TEXT NOT NULL,
            ano_emplacamento INTEGER NOT NULL,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()  
    conn.close()


class VeiculosService(MQTTServiceBase):
    def __init__(self, broker_host="mosquitto"):
        super().__init__("ServicoVeiculos", broker_host=broker_host)
        init_db()

        # registra os tópicos que esse serviço escuta
        self.registrar_handler("denatran/veiculo/emplacar/req", self.emplacar_veiculo)
        self.registrar_handler("denatran/veiculo/ipva/req", self.calcular_ipva)
        self.registrar_handler("denatran/veiculo/transferir/req", self.transferir_proprietario)
        self.registrar_handler("denatran/veiculo/por_ano/req", self.veiculos_por_ano)
        self.registrar_handler("denatran/veiculo/consultar/req", self.consultar_veiculo)

    def emplacar_veiculo(self, dados: dict, envelope: dict) -> dict:
        """
        Cadastra um novo veículo no sistema.
        Entrada esperada em dados:
            {"placa": "ABC1D23", "modelo": "Civic", "valor": 100000.0, "cpf_condutor": "111...", "ano": 2024}
        Retorno de sucesso:
            {"status": "sucesso", "mensagem": "Veículo placa ABC1D23 emplacado com sucesso!"}
        Retorno de erro:
            {"status": "erro", "mensagem": "..."}
        """
        placa = str(dados.get("placa", "")).strip().upper()
        modelo = str(dados.get("modelo", "")).strip()
        try:
            valor = float(dados.get("valor", 0.0))
        except (ValueError, TypeError):
            valor = 0.0
        cpf_condutor = str(dados.get("cpf_condutor", "")).strip()
        try:
            ano = int(dados.get("ano", 0))
        except (ValueError, TypeError):
            ano = 0

        if not placa or not modelo or valor <= 0 or not cpf_condutor:
            return {"status": "erro", "mensagem": "Dados inválidos para emplacamento."}

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO veiculos (placa, modelo, valor, cpf_condutor, ano_emplacamento) VALUES (?, ?, ?, ?, ?)",
                (placa, modelo, valor, cpf_condutor, ano)
            )
            conn.commit()
            return {"status": "sucesso", "mensagem": f"Veículo placa {placa} emplacado com sucesso!"}
        except sqlite3.IntegrityError:
            return {"status": "erro", "mensagem": f"Veículo com placa {placa} já está cadastrado."}
        finally:
            conn.close()

    def calcular_ipva(self, dados: dict, envelope: dict) -> dict:
        """
        Calcula o IPVA de um veículo aplicando a alíquota fixa de 2% sobre o valor venal.
        Entrada esperada em dados:
            {"placa": "ABC1D23"}
        Retorno de sucesso:
            {
                "status": "sucesso",
                "placa": "ABC1D23",
                "modelo": "Civic",
                "valor_venal": 100000.0,
                "aliquota": "2%",
                "valor_ipva": 2000.0
            }
        """
        placa = str(dados.get("placa", "")).strip().upper()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()        
        cursor.execute("SELECT valor, modelo FROM veiculos WHERE placa = ?", (placa,))
        resultado = cursor.fetchone() 
        conn.close()

        if resultado is not None:
            valor = resultado[0]
            modelo = resultado[1]
            valor_ipva = round(valor * 0.02, 2)

            return {
                "status": "sucesso",
                "placa": placa,
                "modelo": modelo,
                "valor_venal": valor,
                "aliquota": "2%",
                "valor_ipva": valor_ipva
            }
        else:
            return {
                "status": "erro",
                "mensagem": f"Veículo com placa {placa} não encontrado."
            }

    def transferir_proprietario(self, dados: dict, envelope: dict) -> dict:
        """
        Transfere a titularidade do veículo para um novo proprietário.
        Entrada esperada em dados:
            {"placa": "ABC1D23", "novo_cpf": "222.222.222-22"}
        Retorno de sucesso:
            {"status": "sucesso", "mensagem": "Propriedade transferida com sucesso."}
        """
        placa = str(dados.get("placa", "")).strip().upper()
        novo_cpf = str(dados.get("novo_cpf", "")).strip()

        if not placa or not novo_cpf:
            return {"status": "erro", "mensagem": "Placa e CPF do novo condutor são obrigatórios."}

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT cpf_condutor FROM veiculos WHERE placa = ?", (placa,))
        resultado = cursor.fetchone()

        if resultado is None:
            conn.close()
            return {"status": "erro", "mensagem": f"Veículo com placa {placa} não encontrado."}

        cursor.execute(
            "UPDATE veiculos SET cpf_condutor = ?, atualizado_em = CURRENT_TIMESTAMP WHERE placa = ?",
            (novo_cpf, placa)
        )
        conn.commit()
        conn.close()

        return {
            "status": "sucesso",
            "mensagem": f"Propriedade do veículo {placa} transferida com sucesso para o CPF {novo_cpf}."
        }

    def veiculos_por_ano(self, dados: dict, envelope: dict) -> dict:
        """
        Lista todos os veículos emplacados em um determinado ano.
        Entrada esperada em dados:
            {"ano": 2024}
        Retorno de sucesso:
            {
                "status": "sucesso",
                "ano": 2024,
                "total": 2,
                "veiculos": [
                    {"placa": "...", "modelo": "...", "valor": 50000.0, "cpf_condutor": "...", "ano": 2024},
                    ...
                ]
            }
        """
        try:
            ano = int(dados.get("ano", 0))
        except (ValueError, TypeError):
            ano = 0

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT placa, modelo, valor, cpf_condutor, ano_emplacamento FROM veiculos WHERE ano_emplacamento = ?",
            (ano,)
        )
        linhas = cursor.fetchall()
        conn.close()

        veiculos = []
        for r in linhas:
            veiculos.append({
                "placa": r[0],
                "modelo": r[1],
                "valor": r[2],
                "cpf_condutor": r[3],
                "ano": r[4]
            })

        return {
            "status": "sucesso",
            "ano": ano,
            "total": len(veiculos),
            "veiculos": veiculos
        }

    def consultar_veiculo(self, dados: dict, envelope: dict) -> dict:
        """
        Consulta os dados de um veículo por placa (utilizado internamente pelo serviço de Multas).
        Entrada esperada em dados:
            {"placa": "ABC1D23"}
        Retorno de sucesso:
            {
                "status": "sucesso",
                "veiculo": {"placa": "...", "modelo": "...", "valor": 1000.0, "cpf_condutor": "...", "ano": 2024}
            }
        """
        placa = str(dados.get("placa", "")).strip().upper()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT placa, modelo, valor, cpf_condutor, ano_emplacamento FROM veiculos WHERE placa = ?",
            (placa,)
        )
        resultado = cursor.fetchone()
        conn.close()

        if resultado is not None:
            return {
                "status": "sucesso",
                "veiculo": {
                    "placa": resultado[0],
                    "modelo": resultado[1],
                    "valor": resultado[2],
                    "cpf_condutor": resultado[3],
                    "ano": resultado[4]
                }
            }
        else:
            return {
                "status": "erro",
                "mensagem": f"Veículo com placa {placa} não encontrado."
            }


if __name__ == "__main__":
    broker = os.environ.get("BROKER_HOST", "localhost")
    service = VeiculosService(broker_host=broker)
    service.iniciar()
