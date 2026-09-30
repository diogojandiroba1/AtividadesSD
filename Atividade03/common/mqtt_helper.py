import json
import logging
import uuid
import threading
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s")


class MQTTServiceBase:
    def __init__(self, service_name: str, broker_host: str = "mosquitto", broker_port: int = 1883):
        self.service_name = service_name
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.logger = logging.getLogger(service_name)
        
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"{service_name}_{uuid.uuid4().hex[:6]}"
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.handlers = {}

    def registrar_handler(self, topico: str, handler_func):
        self.handlers[topico] = handler_func

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            self.logger.error(f"Falha ao conectar no broker MQTT: {reason_code}")
        else:
            self.logger.info(f"Conectado ao Broker MQTT em {self.broker_host}:{self.broker_port}")
            for topico in self.handlers.keys():
                client.subscribe(topico, qos=1)
                self.logger.info(f"Inscrito com sucesso no tópico: {topico}")

    def _on_message(self, client, userdata, msg):
        # Despacha o processamento em uma thread separada para não bloquear
        # o loop de rede do Paho MQTT quando um handler faz chamadas RPC síncronas
        threading.Thread(target=self._executar_handler, args=(msg,), daemon=True).start()

    def _executar_handler(self, msg):
        topico = msg.topic
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
        except Exception as e:
            self.logger.error(f"Erro ao decodificar JSON recebido em {topico}: {e}")
            return

        handler = self.handlers.get(topico)
        if handler:
            try:
                resposta = handler(payload.get("dados", {}), payload)
                reply_to = payload.get("reply_to")
                correlation_id = payload.get("correlation_id")

                if reply_to and correlation_id and resposta is not None:
                    envelope = {
                        "correlation_id": correlation_id,
                        **resposta
                    }
                    self.publicar(reply_to, envelope)
            except Exception as e:
                self.logger.exception(f"Erro ao executar handler para {topico}: {e}")
                reply_to = payload.get("reply_to")
                correlation_id = payload.get("correlation_id")
                if reply_to and correlation_id:
                    self.publicar(reply_to, {
                        "correlation_id": correlation_id,
                        "status": "erro",
                        "mensagem": f"Erro interno no serviço: {str(e)}"
                    })
        else:
            self.logger.warning(f"Nenhum handler registrado para o tópico {topico}")

    def publicar(self, topico: str, dados_dict: dict, qos: int = 1):
        payload_bytes = json.dumps(dados_dict).encode('utf-8')
        self.client.publish(topico, payload_bytes, qos=qos)

    def iniciar(self):
        self.logger.info(f"Iniciando serviço {self.service_name}...")
        self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        self.client.loop_forever()