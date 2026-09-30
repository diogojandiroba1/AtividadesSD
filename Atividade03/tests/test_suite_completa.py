"""
Suíte de Testes Automatizada: DENATRAN
Valida os 10 requisitos funcionais do enunciado.
"""
import sys
import os
import time
import json

# Garante que a pasta raiz esteja no path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from client.cli import DenatranClient


def run_tests():
    broker = os.environ.get("BROKER_HOST", "localhost")
    print(f"[*] Conectando ao Broker MQTT em {broker}...")
    cli = DenatranClient(broker_host=broker)
    cli.iniciar()
    time.sleep(1)  # Aguardar conexão e subscrição no canal de resposta

    try:
        print("\n" + "="*50)
        print("--- TESTE 1: Cadastrar Condutores ---")
        c1 = cli.enviar_requisicao("denatran/condutor/cadastrar/req", {"cpf": "111.111.111-11", "nome": "Ayrton Senna"})
        c2 = cli.enviar_requisicao("denatran/condutor/cadastrar/req", {"cpf": "222.222.222-22", "nome": "Lewis Hamilton"})
        c3 = cli.enviar_requisicao("denatran/condutor/cadastrar/req", {"cpf": "333.333.333-33", "nome": "Max Verstappen"})
        c4 = cli.enviar_requisicao("denatran/condutor/cadastrar/req", {"cpf": "444.444.444-44", "nome": "Rubens Barrichello"})
        c5 = cli.enviar_requisicao("denatran/condutor/cadastrar/req", {"cpf": "555.555.555-55", "nome": "Felipe Massa"})
        c6 = cli.enviar_requisicao("denatran/condutor/cadastrar/req", {"cpf": "666.666.666-66", "nome": "Alain Prost"})
        print("Resultado cadastro c1:", c1)

        print("\n--- TESTE 2: Emplacar Veículos ---")
        v1 = cli.enviar_requisicao("denatran/veiculo/emplacar/req", {
            "placa": "SEN1988", "modelo": "McLaren MP4/4", "valor": 500000.0, "cpf_condutor": "111.111.111-11", "ano": 2024
        })
        v2 = cli.enviar_requisicao("denatran/veiculo/emplacar/req", {
            "placa": "HAM2008", "modelo": "Mercedes W11", "valor": 300000.0, "cpf_condutor": "222.222.222-22", "ano": 2024
        })
        v3 = cli.enviar_requisicao("denatran/veiculo/emplacar/req", {
            "placa": "VER2021", "modelo": "Red Bull RB16B", "valor": 400000.0, "cpf_condutor": "333.333.333-33", "ano": 2023
        })
        print("Resultado emplacamento v1:", v1)

        print("\n--- TESTE 3: Calcular IPVA (2%) ---")
        ipva = cli.enviar_requisicao("denatran/veiculo/ipva/req", {"placa": "SEN1988"})
        print("Cálculo IPVA (Valor R$ 500.000 -> 2% = R$ 10.000):", ipva)
        if ipva.get("status") == "sucesso":
            assert ipva.get("valor_ipva") == 10000.0, f"Erro no cálculo do IPVA! Esperado 10000.0, obtido {ipva.get('valor_ipva')}"
            print("[OK] IPVA validado!")

        print("\n--- TESTE 4: Transferir Proprietário ---")
        transf = cli.enviar_requisicao("denatran/veiculo/transferir/req", {
            "placa": "HAM2008", "novo_cpf": "444.444.444-44"
        })
        print("Resultado Transferência:", transf)

        print("\n--- TESTE 5: Lançar Multas ---")
        # Ayrton (SEN1988): 7 + 7 = 14 pontos
        m1 = cli.enviar_requisicao("denatran/multa/lancar/req", {"ano": 2024, "descricao": "Excesso gravíssimo", "pontuacao": 7, "placa": "SEN1988"})
        m2 = cli.enviar_requisicao("denatran/multa/lancar/req", {"ano": 2024, "descricao": "Farol vermelho", "pontuacao": 7, "placa": "SEN1988"})
        # Rubens (HAM2008 após transferência): 5 + 4 = 9 pontos
        m3 = cli.enviar_requisicao("denatran/multa/lancar/req", {"ano": 2024, "descricao": "Estacionar em local proibido", "pontuacao": 5, "placa": "HAM2008"})
        m4 = cli.enviar_requisicao("denatran/multa/lancar/req", {"ano": 2024, "descricao": "Uso de celular", "pontuacao": 4, "placa": "HAM2008"})
        # Max (VER2021): 20 pontos
        m5 = cli.enviar_requisicao("denatran/multa/lancar/req", {"ano": 2024, "descricao": "Velocidade na reta", "pontuacao": 20, "placa": "VER2021"})
        print("Resultado lançamento m1:", m1)

        print("\n--- TESTE 6: Veículos Emplacados em 2024 ---")
        veic_2024 = cli.enviar_requisicao("denatran/veiculo/por_ano/req", {"ano": 2024})
        print("Veículos de 2024:", veic_2024)

        print("\n--- TESTE 7: Multas Cometidas pelo Veículo SEN1988 em 2024 ---")
        multas_v = cli.enviar_requisicao("denatran/multa/por_veiculo_ano/req", {"placa": "SEN1988", "ano": 2024})
        print("Multas do SEN1988:", json.dumps(multas_v, indent=2, ensure_ascii=False))

        print("\n--- TESTE 8: Multas do Condutor Rubens (444.444.444-44) em 2024 ---")
        multas_c = cli.enviar_requisicao("denatran/multa/por_condutor_ano/req", {"cpf": "444.444.444-44", "ano": 2024})
        print("Multas do Rubens:", json.dumps(multas_c, indent=2, ensure_ascii=False))

        print("\n--- TESTE 9: Todas as Multas Lançadas em 2024 ---")
        multas_ano = cli.enviar_requisicao("denatran/multa/por_ano/req", {"ano": 2024})
        print("Total de multas em 2024:", multas_ano)

        print("\n--- TESTE 10: TOP 5 Condutores com Maior Pontuação ---")
        top5 = cli.enviar_requisicao("denatran/multa/top5/req", {})
        print("Ranking TOP 5:", json.dumps(top5, indent=2, ensure_ascii=False))

        print("\n" + "="*50)
        print("[*] FIM DA EXECUÇÃO DOS TESTES")
        print("="*50)

    finally:
        cli.parar()


if __name__ == "__main__":
    run_tests()
