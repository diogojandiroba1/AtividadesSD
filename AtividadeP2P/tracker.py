from http.server import BaseHTTPRequestHandler, HTTPServer
import re
import socket


peers_db = {}


class BitTorrentTracker(BaseHTTPRequestHandler):

  def do_GET(self):
    if not self.path.startswith('/announce'):
      self.send_error(404)
      return

    query = self.path.split('?', 1)[1] if '?' in self.path else ''
    hash_match = re.search(r'info_hash=([^&]+)', query)
    port_match = re.search(r'port=(\d+)', query)

    if not hash_match or not port_match:
      self.send_error(400)
      return

    info_hash = hash_match.group(1)
    porta_cliente = int(port_match.group(1))
    ip_cliente = self.client_address[0]

    if info_hash not in peers_db:
      peers_db[info_hash] = set()

    # Registra o nó solicitante
    peers_db[info_hash].add((ip_cliente, porta_cliente))

    # Formato BitTorrent 
    peers_bin = b''.join(
        socket.inet_aton(ip) + p.to_bytes(2, 'big')
        for ip, p in peers_db[info_hash]
    )

  
    resposta = (
        b'd8:intervali60e5:peers'
        + str(len(peers_bin)).encode()
        + b':'
        + peers_bin
        + b'e'
    )

    self.send_response(200)
    self.send_header('Content-Type', 'text/plain')
    self.send_header('Content-Length', str(len(resposta)))
    self.end_headers()
    self.wfile.write(resposta)

  def log_message(self, format, *args):
    pass


if __name__ == '__main__':
  server = HTTPServer(('127.0.0.1', 6969), BitTorrentTracker)
  print('Tracker P2P escutando em http://127.0.0.1:6969/announce...')
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    server.server_close()