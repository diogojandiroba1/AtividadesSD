import socket
import threading
from concurrent.futures import ThreadPoolExecutor
import time
import sys

modo = sys.argv[1]
caminho_arquivo = sys.argv[2]

addr = ("", 9000)  
s = socket.create_server(addr)
print("Servidor aguardando conexões na porta 9000...")

def atender_cliente(conn, cliente_addr, caminho):
  print(f"Cliente conectado: {cliente_addr}")
  try:
    with open(caminho, "rb") as f:
      conn.sendfile(f)
  finally:
    conn.close()
    print(f"Conexão com {cliente_addr} finalizada.")

if modo == 'iterativo':
    while True:
        conn, cliente_addr = s.accept()
        print(f"Cliente conectado: {cliente_addr}")

        with open(caminho_arquivo, "rb") as f:
             conn.sendfile(f)  

        conn.close()
        print("Envio concluído e conexão fechada")

if modo == "thread":
  while True:
    conn, cliente_addr = s.accept()

    t = threading.Thread(
        target=atender_cliente, args=(conn, cliente_addr, caminho_arquivo)
    )
    t.start()


if modo == "pool":
  limite_n = int(sys.argv[3]) if len(sys.argv) > 3 else 5
  print(f"Executando com pool de {limite_n} threads...")

  with ThreadPoolExecutor(max_workers=limite_n) as executor:
    while True:
      conn, cliente_addr = s.accept()
      executor.submit(
          atender_cliente, conn, cliente_addr, caminho_arquivo
      )

else:
  print(f'Modo "{modo}" inválido. Escolha entre: iterativo, thread, pool.')
  s.close()
  sys.exit(1)
