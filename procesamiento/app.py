from kafka import KafkaConsumer, KafkaProducer
import json
import time

consumer = KafkaConsumer(
    'pedidos',  # Tópico donde se reciben los pedidos originales
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    group_id='group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Configuración del productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def actualizar_estado(pedido):
    if 'estado' not in pedido:
        pedido['estado'] = "recibido"  # inicial
    elif pedido['estado'] == "recibido":
        pedido['estado'] = "preparando"
    elif pedido['estado'] == "preparando":
        pedido['estado'] = "entregando"
    elif pedido['estado'] == "entregando":
        pedido['estado'] = "finalizado"
    return pedido

for mensaje in consumer:
    lista_pedidos = mensaje.value 
    for pedido in lista_pedidos:
        pedido_act = actualizar_estado(pedido)
        producer.send('pedidos', value=pedido_act)
        producer.flush()
    
    time.sleep(5) 
