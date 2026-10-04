import pika
from PIL import Image
import io
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
canal.queue_declare(queue='fila_imagens_brutas', durable=True)

canal.exchange_declare(exchange='imagens_convertidas', exchange_type='fanout')


def callback(ch, method, properties, body):
    print("Imagem recebida para conversão!")
    
    img = Image.open(io.BytesIO(body))
    img_cinza = img.convert('L') # Converte para tons de cinza
    buffer = io.BytesIO()
    img_cinza.save(buffer, format=img.format or 'JPEG')
    imagem_cinza_bytes = buffer.getvalue()

    ch.basic_publish(
        exchange='imagens_convertidas',
        routing_key='',  # fanout não usa routing_key
        body=imagem_cinza_bytes,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            headers=properties.headers # repassa o nome original do arquivo!
        )
    )
    print("Imagem convertida enviada...")


    # (ACK): Diz ao RabbitMQ que a mensagem foi tratada e já pode ser removida da fila.
    ch.basic_ack(delivery_tag=method.delivery_tag)


# o rabbitmq só manda outra imagem quando ele terminar a atual (processar)
canal.basic_qos(prefetch_count=1)

canal.basic_consume(
    queue='fila_imagens_brutas',
    on_message_callback = callback
)

print("Aguardando Imagens...")
canal.start_consuming() #loop escutando o rabbitmq