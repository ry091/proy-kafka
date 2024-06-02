from flask import Flask, jsonify, request
from kafka import KafkaConsumer, KafkaProducer
import json
import threading
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

app = Flask(__name__)

# Configuración del productor de Kafka
producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Diccionario para mantener el estado de los pedidos
pedidos_estado = {}

# Función para modificar los datos antes de enviarlos
def datos(data):
    ct = 1
    for dato in data:
        dato['id'] = ct
        ct += 1
    return data

# Función para enviar correos electrónicos
def enviar_correo(pedido, estado):
    # Configuración de los detalles del correo electrónico
    remitente = 'email'
    destinatario = 'emaildest'
    asunto = 'Estado del pedido'
    cuerpo = f'El estado del pedido {pedido["id"]}, nombre: {pedido["nombre"]}, precio: {pedido["precio"]} es: {estado}'

    # Configuración del servidor SMTP
    servidor_smtp = 'smtp.gmail.com'
    puerto_smtp = 587
    usuario = 'email'
    contrasena = 'contrasena'

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

# Función para enviar notificaciones por correo electrónico con detalles completos del pedido
def enviar_notificacion(pedido):
    estado = pedido["estado"]
    enviar_correo(pedido, estado)

# Función para manejar el estado del pedido y enviar notificaciones por correo
def manejar_estado_pedido():
    consumer = KafkaConsumer('pedidos_actualizados', bootstrap_servers=['kafka:9092'], auto_offset_reset='earliest', group_id='group', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for mensaje in consumer:
        pedido = mensaje.value
        pe_id = int(pedido.get('id'))
        if pe_id:
            # Actualizar estado del pedido
            pedidos_estado[pe_id] = pedido
            # Enviar notificación por correo electrónico
            enviar_notificacion(pedido)

# Iniciar el hilo para manejar el estado del pedido
threading.Thread(target=manejar_estado_pedido, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


