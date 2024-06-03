from kafka import KafkaConsumer, KafkaProducer
import json
import time

# Configurar el consumidor de Kafka
consumer = KafkaConsumer(
    'pedidos',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    group_id='group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Configurar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic_procesado = 'pedidos_procesados'
topic_metricas = 'metricas_procesamiento'

# Tiempos de retraso entre cada estado
DELAY_RECIBIDO = 2
DELAY_PREPARANDO = 5
DELAY_ENTREGANDO = 10

def enviar_metricas(pedido_id, estado, start_time, end_time):
    duracion = end_time - start_time
    metricas = {
        'id': pedido_id,
        'estado': estado,
        'inicio': start_time,
        'fin': end_time,
        'duracion': duracion
    }
    producer.send(topic_metricas, value=metricas)

for msg in consumer:
    lista_pedidos = msg.value  # Asumiendo que msg.value es una lista de pedidos

    for pedido in lista_pedidos:
        pedido_id = pedido['id']
        
        # Estado: recibido
        start_time = time.time()
        pedido['estado'] = 'recibido'
        producer.send(topic_procesado, value=pedido)
        time.sleep(DELAY_RECIBIDO)
        end_time = time.time()
        enviar_metricas(pedido_id, 'recibido', start_time, end_time)

        # Estado: preparando
        start_time = time.time()
        pedido['estado'] = 'preparando'
        producer.send(topic_procesado, value=pedido)
        time.sleep(DELAY_PREPARANDO)
        end_time = time.time()
        enviar_metricas(pedido_id, 'preparando', start_time, end_time)

        # Estado: entregando
        start_time = time.time()
        pedido['estado'] = 'entregando'
        producer.send(topic_procesado, value=pedido)
        time.sleep(DELAY_ENTREGANDO)
        end_time = time.time()
        enviar_metricas(pedido_id, 'entregando', start_time, end_time)

        # Estado: finalizado
        start_time = time.time()
        pedido['estado'] = 'finalizado'
        producer.send(topic_procesado, value=pedido)
        end_time = time.time()
        enviar_metricas(pedido_id, 'finalizado', start_time, end_time)
