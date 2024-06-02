from kafka import KafkaConsumer, KafkaProducer
import json
import time

# Configurar el consumidor de Kafka
consumer = KafkaConsumer('solicitudes', bootstrap_servers='192.168.128.3:9092')

# Configurar el productor de Kafka
producer = KafkaProducer(bootstrap_servers='kafka:9092')
topic_procesado = 'pedidos_procesados'

# Tiempo de retraso entre cada estado (en segundos)
DELAY_RECIBIDO = 2
DELAY_PREPARANDO = 5
DELAY_ENTREGANDO = 10

for msg in consumer:
    pedido = json.loads(msg.value)

    # Estado: recibido
    pedido['estado'] = 'recibido'
    producer.send(topic_procesado, value=json.dumps(pedido).encode('utf-8'))
    print(f'Pedido procesado: {pedido}')
    time.sleep(DELAY_RECIBIDO)

    # Estado: preparando
    pedido['estado'] = 'preparando'
    producer.send(topic_procesado, value=json.dumps(pedido).encode('utf-8'))
    print(f'Pedido procesado: {pedido}')
    time.sleep(DELAY_PREPARANDO)

    # Estado: entregando
    pedido['estado'] = 'entregando'
    producer.send(topic_procesado, value=json.dumps(pedido).encode('utf-8'))
    print(f'Pedido procesado: {pedido}')
    time.sleep(DELAY_ENTREGANDO)

    # Estado: finalizado
    pedido['estado'] = 'finalizado'
    producer.send(topic_procesado, value=json.dumps(pedido).encode('utf-8'))
    print(f'Pedido procesado: {pedido}')
