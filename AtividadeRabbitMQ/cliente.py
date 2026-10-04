import pika 
import os  
import time


def conectar_rabbitmq():
    while True:
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        except pika.exceptions.AMQPConnectionError:
            print("Aguardando RabbitMQ inicializar...")
            time.sleep(2)
conexao = conectar_rabbitmq()
canal = conexao.channel()
 
time.sleep(5)

pasta_imagens = '/app/imagens'

canal.queue_declare(queue='fila_imagens_brutas', durable=True)

if os.path.exists(pasta_imagens):
    for nome_arquivo in os.listdir(pasta_imagens):
        caminho_completo = os.path.join(pasta_imagens, nome_arquivo)
        
        if os.path.isfile(caminho_completo):
            with open(caminho_completo, 'rb') as f:
                dados_imagem = f.read()
            canal.basic_publish(
                exchange='',
                routing_key='fila_imagens_brutas',
                body=dados_imagem,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    headers={'filename': nome_arquivo}
                )
            )
            print(f"Imagem enviada: {nome_arquivo}")
            time.sleep(1) 

print("Imagem enviada")
conexao.close()