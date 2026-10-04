import pika
from PIL import Image
import io
import os
import time

nome_fila = os.getenv('NOME_FILA', 'fila_storage_default')

def conectar_rabbitmq():
    while True:
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        except pika.exceptions.AMQPConnectionError:
            print("Aguardando RabbitMQ inicializar...")
            time.sleep(2)
conexao = conectar_rabbitmq()
canal = conexao.channel()
 
canal.exchange_declare(exchange='imagens_convertidas', exchange_type='fanout')

canal.queue_declare(queue=nome_fila, durable=True)

canal.queue_bind(exchange='imagens_convertidas', queue=nome_fila)

def salvar_imagem(ch, method, properties, body):
    nome_arquivo = 'imagem_convertida.jpg'
    if properties and properties.headers and 'filename' in properties.headers:
        nome_arquivo = properties.headers['filename']
    
    # Caminho na pasta montada pelo volume
    caminho = os.path.join('/app/dados_salvos', nome_arquivo)
    
    # Grava os bytes diretamente no arquivo
    with open(caminho, 'wb') as f:
        f.write(body)
        
    print(f"[{nome_fila}] Imagem '{nome_arquivo}' salva com sucesso!")
    ch.basic_ack(delivery_tag=method.delivery_tag)

canal.basic_consume(queue=nome_fila, on_message_callback=salvar_imagem)
canal.start_consuming()