from concurrent.futures import ThreadPoolExecutor
import os
import shutil
import socket
import subprocess
import sys
import threading
import time


# Testes


def cliente_socket(servidor_ip, porta, barreira):
  barreira.wait()  # Aguarda todos os clientes estarem prontos para disparar

  t_inicio = time.perf_counter()
  try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((servidor_ip, porta))

    while True:
      dados = s.recv(65536)
      if not dados:
        break
    s.close()
    t_fim = time.perf_counter()
    return t_fim - t_inicio
  except Exception as e:
    print(f'Erro no cliente: {e}')
    return None


def benchmark_cliente_servidor(servidor_ip, porta, num_clientes):
  barreira = threading.Barrier(num_clientes)
  tempos = []

  with ThreadPoolExecutor(max_workers=num_clientes) as executor:
    futures = [
        executor.submit(cliente_socket, servidor_ip, porta, barreira)
        for _ in range(num_clientes)
    ]
    for f in futures:
      res = f.result()
      if res is not None:
        tempos.append(res)

  return tempos


def cliente_p2p(torrent_path, peer_id):
  """Executa um nó cliente do aria2c num diretório isolado."""
  pasta_download = f'/tmp/peer_{peer_id}'
  os.makedirs(pasta_download, exist_ok=True)

  cmd = [
      'aria2c',
      '--enable-dht=false',
      '--seed-time=0',
      '--summary-interval=0',
      f'--dir={pasta_download}',
      torrent_path,
  ]

  t_inicio = time.perf_counter()
  subprocess.run(
      cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True
  )
  t_fim = time.perf_counter()

  # Remove ficheiros descarregados para não ocupar disco
  shutil.rmtree(pasta_download, ignore_errors=True)
  return t_fim - t_inicio


def benchmark_p2p(torrent_path, num_clientes):
  tempos = []
  with ThreadPoolExecutor(max_workers=num_clientes) as executor:
    futures = [
        executor.submit(cliente_p2p, torrent_path, i)
        for i in range(num_clientes)
    ]
    for f in futures:
      try:
        tempos.append(f.result())
      except Exception as e:
        print(f'Erro no cliente P2P: {e}')
  return tempos


def exibir_resultados(tempos):
  if not tempos:
    print('Nenhum dado recolhido.')
    return

  t_min = min(tempos)
  t_med = sum(tempos) / len(tempos)
  t_max = max(tempos)

  print('RESULTADOS DO EXPERIMENTO')
  print(f'Clientes concluídos: {len(tempos)}')
  print(f'Tempo Mínimo:       {t_min:.4f} s')
  print(f'Tempo Médio:        {t_med:.4f} s')
  print(f'Tempo Máximo:       {t_max:.4f} s')
  print('=' * 40 + '\n')


if __name__ == '__main__':
  if len(sys.argv) < 3:
    print('Uso:')
    print(
        '  Cliente-Servidor: python3 benchmark.py cs <num_clientes> [ip_servidor]'
        ' [porta]'
    )
    print('  P2P:              python3 benchmark.py p2p <num_clientes> <torrent>')
    sys.exit(1)

  tipo = sys.argv[1].lower()
  n_clientes = int(sys.argv[2])

  if tipo == 'cs':
    ip = sys.argv[3] if len(sys.argv) > 3 else '127.0.0.1'
    porta = int(sys.argv[4]) if len(sys.argv) > 4 else 9000
    print(
        f'Disparando {n_clientes} conexões cliente-servidor para {ip}:{porta}...'
    )
    tempos = benchmark_cliente_servidor(ip, porta, n_clientes)
    exibir_resultados(tempos)

  elif tipo == 'p2p':
    if len(sys.argv) < 4:
      print('Indique o caminho do ficheiro .torrent para o teste P2P.')
      sys.exit(1)
    caminho_torrent = sys.argv[3]
    print(
        f'Disparando {n_clientes} pares P2P via aria2c com {caminho_torrent}...'
    )
    tempos = benchmark_p2p(caminho_torrent, n_clientes)
    exibir_resultados(tempos)