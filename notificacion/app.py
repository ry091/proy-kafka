from flask import Flask, jsonify, send_from_directory, abort
from kafka import KafkaConsumer
import json
import smtplib
import os
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

app = Flask(__name__)

consumer = KafkaConsumer(
    'pedidos_procesados', 
    bootstrap_servers=os.getenv('KAFKA_SERVER', 'kafka:9092'),
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
consumer_metricas_procesamiento = KafkaConsumer(
    'metricas_procesamiento',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    group_id='group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
# Almacenar los pedidos procesados
pedidos_estado = {}

def enviar_correo(pedido, estado):
    remitente = 'halaoa109@gmail.com'
    destinatario = 'halaoa109@gmail.com'
    asunto = 'Estado del pedido'
    cuerpo = f'El estado del pedido {pedido["id"]}, nombre: {pedido["nombre"]}, precio: {pedido["precio"]} con estado: {estado}'

    # Configuración del servidor SMTP
    servidor_smtp = 'smtp.gmail.com'
    puerto_smtp = 587
    usuario = 'rayen.millaman@mail.udp.cl'
    contrasena = 'black.0912'

    # Crear el objeto del mensaje
    mensaje = MIMEMultipart()
    mensaje['From'] = remitente
    mensaje['To'] = destinatario
    mensaje['Subject'] = asunto

    # Agregar el cuerpo del mensaje
    mensaje.attach(MIMEText(cuerpo, 'plain'))

    # Iniciar conexión con el servidor SMTP
    servidor = smtplib.SMTP(servidor_smtp, puerto_smtp)
    servidor.starttls()
    servidor.login(usuario, contrasena)
    # Enviar el correo electrónico
    servidor.send_message(mensaje)
    # Cerrar la conexión con el servidor SMTP
    servidor.quit()

def enviar_notificacion(pedido):
    estado = pedido["estado"]
    enviar_correo(pedido, estado)

def consumir_pedidos():
    for mensaje in consumer:
        pedido = mensaje.value
        pedidos_estado[str(pedido['id'])] = pedido
        enviar_notificacion(pedido)

def procesar_metricas_procesamiento():
    with open('metricas.log', 'a') as log_file:
        for msg in consumer_metricas_procesamiento:
            metricas = msg.value
            log_entry = f"{metricas['id']}|{metricas['estado']}|{metricas['inicio']}|{metricas['fin']}|{metricas['duracion']}\n"
            log_file.write(log_entry)
            log_file.flush()
            

threading.Thread(target=consumir_pedidos, daemon=True).start()
threading.Thread(target=procesar_metricas_procesamiento, daemon=True).start()

@app.route('/pedido/<int:id>', methods=['GET'])
def obtener_pedido(id):
    pedido = pedidos_estado.get(str(id))
    if pedido:
        return jsonify(pedido)
    else:
        return jsonify({'error': 'Pedido no encontrado'}), 404

@app.route('/todos', methods=['GET'])
def get_pedidos():
    return jsonify(list(pedidos_estado.values()))

@app.route('/metricas', methods=['GET'])
def get_metricas():
    with open('metricas.log', 'r') as log_file:
        metricas = log_file.readlines()
    metricas = [line.strip().split('|') for line in metricas]
    return jsonify(metricas)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
