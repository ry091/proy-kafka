from kafka import KafkaConsumer, KafkaProducer
import json
import time

# Configuración del consumidor de Kafka
consumer = KafkaConsumer(
    'pedidos',  # Asume que los pedidos se envían inicialmente a este tópico y se re-envían aquí
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    group_id='process-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Configuración del productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
def actualizar_estado(pedido):
    if 'estado' not in pedido:
        pedido['estado'] = "recibido"  # Estado inicial
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
            time.sleep(1)  # Simular tiempo de procesamiento
        # Envía el pedido actualizado al tópico 'pedidos_finalizados' solo cuando está "finalizado"
        producer.send('pedidos_actualizados', value=pedido)  # Asegúrate de que 'pedido' ya es un diccionario
        producer.flush()
    time.sleep(5)
