from flask import Flask, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
ct=1
def datos():
    with open('dataset.json', 'r') as file:
        datos = json.load(file)
        for dato in datos:
            dato['id'] =   ct
            ct+=1
            producer.send('pedidos', value=dato)

@app.route('/cargar_datos', methods=['POST'])
def cargar_datos():
    datos()
    return jsonify({'mensaje': 'ok'}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
