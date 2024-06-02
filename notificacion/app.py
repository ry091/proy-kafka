from flask import Flask, jsonify
from kafka import KafkaConsumer
import threading
import json

app = Flask(__name__)

pedidos_estado = {}
consumer = KafkaConsumer(
    'pedidos_actualizados', 
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    group_id='group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def consumir_pedidos():
    for mensaje in consumer:
        pedido = mensaje.value
        pe_id = int(pedido.get('id'))
        if pe_id:
            pedidos_estado[pe_id] = pedido

# Iniciar el hilo para consumir mensajes de Kafka
threading.Thread(target=consumir_pedidos, daemon=True).start()

@app.route('/pedido/<int:id>', methods=['GET'])
def obtener_pedido(id):
    pedido = pedidos_estado.get(id)
    if pedido:
        return jsonify(pedido)
    else:
        return jsonify({'error': 'Pedido no encontrado'}), 404

@app.route('/todos', methods=['GET'])
def get_pedidos():
    return jsonify(list(pedidos_estado.values()))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
