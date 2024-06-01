from flask import Flask, jsonify, request
from kafka import KafkaConsumer
import threading
import json

app = Flask(__name__)

# Diccionario para almacenar los estados de los pedidos
pedidos = {}

def kafka_consumer():
    consumer = KafkaConsumer(
        'pedidos',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        group_id='notificacion-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    for mensaje in consumer:
        pedido = mensaje.value
        pedidos[pedido['id']] = pedido

# Iniciar el consumidor de Kafka en un hilo separado para que no bloquee el servidor web
threading.Thread(target=kafka_consumer).start()

@app.route('/pedido/<id>', methods=['GET'])
def get_pedido(id):
    # Buscar el pedido por ID y devolverlo
    pedido = pedidos.get(id)
    if pedido:
        return jsonify(pedido)
    else:
        return jsonify({'error': 'Pedido no encontrado'}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
