from flask import Flask, jsonify
from kafka import KafkaConsumer
import json
import smtplib

app = Flask(__name__)

# Configurar el consumidor de Kafka
consumer = KafkaConsumer('pedidos_procesados', bootstrap_servers='192.168.128.3:9092')

# Almacenar los pedidos procesados
pedidos = {}

# Configuración del servidor SMTP (ejemplo con Gmail)
smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_username = 'felipe.mora_m@mail.udp.cl'
smtp_password = 'Pc4Gc4-T'

def enviar_notificacion(pedido):
    mensaje = f"Estado del pedido: {pedido['id']}\n"
    mensaje += f"Nombre: {pedido['nombre']}\n"
    mensaje += f"Precio: ${pedido['precio']}\n"
    mensaje += f"Estado: {pedido['estado']}\n"

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, pedido['correo'], mensaje)
        print(f"Notificación enviada a {pedido['correo']}")
    except Exception as e:
        print(f"Error al enviar notificación: {e}")
    finally:
        server.quit()

for msg in consumer:
    pedido = json.loads(msg.value)
    pedidos[pedido['id']] = pedido

    # Enviar notificación por correo electrónico
    enviar_notificacion(pedido)

@app.route('/estado/<id>', methods=['GET'])
def get_estado(id):
    if id in pedidos:
        return jsonify(pedidos[id])
    else:
        return jsonify({"error": "Pedido no encontrado"}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
