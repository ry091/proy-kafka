from flask import Flask, jsonify, request
from kafka import KafkaProducer
import json

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def datos(data):
    ct = 1
    for dato in data:
        dato['id'] = ct
        ct += 1
    return data

@app.route('/cargar_datos', methods=['POST'])
def cargar_datos():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'no hay datos'}), 400
    valor=datos(data)
    producer.send('pedidos', value=valor)
    producer.flush()
    return jsonify({'mensaje': 'ok', 'data':valor}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

