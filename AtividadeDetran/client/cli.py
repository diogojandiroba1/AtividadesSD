"""
Interface de Linha de Comando (CLI) DENATRAN
Responsabilidade: Menu interativo cobrindo os 10 requisitos do enunciado.
"""
import json
import os
import sys
import threading
import uuid
import paho.mqtt.client as mqtt


class DenatranClient:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = f"denatran_cli_{uuid.uuid4().hex[:6]}"
        self.canal_respostas = f"denatran/respostas/{self.client_id}"
        self.pendentes = {}

        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.client_id
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if not reason_code.is_failure:
            client.subscribe(self.canal_respostas, qos=1)

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            corr_id = payload.get("correlation_id")
            if corr_id in self.pendentes:
                self.pendentes[corr_id]["resultado"] = payload
                self.pendentes[corr_id]["evento"].set()
        except Exception:
            pass

    def iniciar(self):
        self.client.connect(self.broker_host, self.broker_port)
        self.client.loop_start()

    def parar(self):
        self.client.loop_stop()
        self.client.disconnect()

    def enviar_requisicao(self, topico: str, dados: dict, timeout: float = 5.0) -> dict:
        corr_id = str(uuid.uuid4())
        evento = threading.Event()
        self.pendentes[corr_id] = {"evento": evento, "resultado": None}

        envelope = {
            "correlation_id": corr_id,
            "reply_to": self.canal_respostas,
            "dados": dados
        }
        self.client.publish(topico, json.dumps(envelope).encode('utf-8'), qos=1)

        if evento.wait(timeout=timeout):
            res = self.pendentes[corr_id]["resultado"]
            del self.pendentes[corr_id]
            return res
        del self.pendentes[corr_id]
        return {"status": "erro", "mensagem": "Tempo limite esgotado (Timeout). O microsserviço está ativo?"}


def imprimir_menu():
    print("SISTEMA NACIONAL DE TRÂNSITO (DENATRAN / MQTT)")
    print("1.  Cadastrar condutor")
    print("2.  Emplacar veículo")
    print("3.  Calcular IPVA do veículo (2%)")
    print("4.  Transferir proprietário de veículo")
    print("5.  Lançar multa")
    print("6.  Informar veículos emplacados em um ano")
    print("7.  Informar multas cometidas por veículo em um ano")
    print("8.  Informar multas de um condutor em um dado ano")
    print("9.  Informar multas lançadas em um dado ano")
    print("10. Informar TOP 5 condutores com maiores pontuações")
    print("0.  Sair")


def main():
    broker = os.environ.get("BROKER_HOST", "localhost")
    cli = DenatranClient(broker_host=broker)
    cli.iniciar()

    try:
        while True:
            imprimir_menu()
            opcao = input("Selecione uma opção (0-10): ").strip()

            if opcao == "0":
                print("Encerrando cliente...")
                break

            elif opcao == "1":
                cpf = input("CPF: ").strip()
                nome = input("Nome: ").strip()
                res = cli.enviar_requisicao("denatran/condutor/cadastrar/req", {"cpf": cpf, "nome": nome})
                print("\n[RESPOSTA]:", json.dumps(res, indent=2, ensure_ascii=False))

            elif opcao == "2":
                placa = input("Placa (ex: ABC1D23): ").strip()
                modelo = input("Modelo: ").strip()
                try:
                    valor = float(input("Valor do Veículo (R$): ").strip())
                except ValueError:
                    print("Valor inválido.")
                    continue
                cpf = input("CPF do condutor: ").strip()
                try:
                    ano = int(input("Ano de emplacamento: ").strip())
                except ValueError:
                    print("Ano inválido.")
                    continue
                res = cli.enviar_requisicao("denatran/veiculo/emplacar/req", {
                    "placa": placa, "modelo": modelo, "valor": valor, "cpf_condutor": cpf, "ano": ano
                })
                print("\n[RESPOSTA]:", json.dumps(res, indent=2, ensure_ascii=False))

            elif opcao == "3":
                placa = input("Placa do veículo: ").strip()
                res = cli.enviar_requisicao("denatran/veiculo/ipva/req", {"placa": placa})
                print("\n[RESPOSTA]:", json.dumps(res, indent=2, ensure_ascii=False))

            elif opcao == "4":
                placa = input("Placa do veículo: ").strip()
                novo_cpf = input("CPF do novo condutor: ").strip()
                res = cli.enviar_requisicao("denatran/veiculo/transferir/req", {"placa": placa, "novo_cpf": novo_cpf})
                print("\n[RESPOSTA]:", json.dumps(res, indent=2, ensure_ascii=False))

            elif opcao == "5":
                try:
                    ano = int(input("Ano da infração: ").strip())
                    descricao = input("Descrição da infração: ").strip()
                    pontuacao = int(input("Pontuação: ").strip())
                except ValueError:
                    print("Dados numéricos inválidos.")
                    continue
                placa = input("Placa do veículo: ").strip()
                res = cli.enviar_requisicao("denatran/multa/lancar/req", {
                    "ano": ano, "descricao": descricao, "pontuacao": pontuacao, "placa": placa
                })
                print("\n[RESPOSTA]:", json.dumps(res, indent=2, ensure_ascii=False))

            elif opcao == "6":
                try:
                    ano = int(input("Ano de emplacamento: ").strip())
                except ValueError:
                    print("Ano inválido.")
                    continue
                res = cli.enviar_requisicao("denatran/veiculo/por_ano/req", {"ano": ano})
                print("\n[RESPOSTA]:", json.dumps(res, indent=2, ensure_ascii=False))

            elif opcao == "7":
                placa = input("Placa do veículo: ").strip()
                ano_str = input("Ano (pressione Enter para todos): ").strip()
                ano = int(ano_str) if ano_str.isdigit() else 0
                res = cli.enviar_requisicao("denatran/multa/por_veiculo_ano/req", {"placa": placa, "ano": ano})
                print("\n[RESPOSTA]:", json.dumps(res, indent=2, ensure_ascii=False))

            elif opcao == "8":
                cpf = input("CPF do condutor: ").strip()
                try:
                    ano = int(input("Ano: ").strip())
                except ValueError:
                    print("Ano inválido.")
                    continue
                res = cli.enviar_requisicao("denatran/multa/por_condutor_ano/req", {"cpf": cpf, "ano": ano})
                print("\n[RESPOSTA]:", json.dumps(res, indent=2, ensure_ascii=False))

            elif opcao == "9":
                try:
                    ano = int(input("Ano: ").strip())
                except ValueError:
                    print("Ano inválido.")
                    continue
                res = cli.enviar_requisicao("denatran/multa/por_ano/req", {"ano": ano})
                print("\n[RESPOSTA]:", json.dumps(res, indent=2, ensure_ascii=False))

            elif opcao == "10":
                res = cli.enviar_requisicao("denatran/multa/top5/req", {})
                print("\n[RESPOSTA]:", json.dumps(res, indent=2, ensure_ascii=False))

            else:
                print("Opção inválida.")
    finally:
        cli.parar()


if __name__ == "__main__":
    main()
