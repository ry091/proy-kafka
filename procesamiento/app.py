from kafka import KafkaConsumer, KafkaProducer
import json
import time

consumer = KafkaConsumer(
    'pedidos',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    group_id='process-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


def actualizar_estado(pedido):
    if 'estado' not in pedido:
        pedido['estado'] = "recibido"  # inicial
    else:
        estados = ["recibido", "preparando", "entregando", "finalizado"]
        current_index = estados.index(pedido['estado'])
        if current_index < len(estados) - 1:
            pedido['estado'] = estados[current_index + 1]
    return pedido


for mensaje in consumer:
    lista_pedidos = mensaje.value
    for pedido in lista_pedidos:
        if 'estado' not in pedido:
            pedido['estado'] = "recibido"
        while pedido['estado'] != "finalizado":
            pedido = actualizar_estado(pedido)
            producer.send('pedidos_actualizados', value=pedido)  # Enviar el pedido actualizado a Kafka
            producer.flush()
            time.sleep(1)  # Esperar un segundo antes de enviar el siguiente estado
    time.sleep(5)  # Esperar 5 segundos antes de revisar el siguiente mensaje del consumidor

