import pika
import dill
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib
from collections import defaultdict
import configuracion
import threading

# Estructuras de datos para las gráficas
stats_clientes = defaultdict(int)  # Cuantos procesó cada cliente
datos_x = []
datos_y = []
progreso_productor = 0

matplotlib.use('TkAgg')

def hilo_consumo_resultados():
    """ Hilo en segundo plano que escucha RabbitMQ """
    connection = pika.BlockingConnection(pika.ConnectionParameters(configuracion.RABBIT_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=configuracion.Q_RESULTADOS)
    channel.queue_declare(queue=configuracion.Q_MONITOREO)

    def callback(ch, method, properties, body):
        global progreso_productor

        if method.routing_key == configuracion.Q_MONITOREO:

            msg = body.decode()
            if "PROD" in msg:
                progreso_productor += 1

        elif method.routing_key == configuracion.Q_RESULTADOS:
            data = dill.loads(body)
            # data es {'cliente': 'pc1', 'valor': 1.5, 'resultado': 2.25}
            stats_clientes[data['cliente']] += 1
            datos_x.append(data['valor'])
            datos_y.append(data['resultado'])

    channel.basic_consume(queue=configuracion.Q_RESULTADOS, on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue=configuracion.Q_MONITOREO, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


# Iniciar consumo en hilo aparte para no bloquear la gráfica
t = threading.Thread(target=hilo_consumo_resultados)
t.daemon = True
t.start()

# Configuración Gráfica
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))


def update(frame):
    # Gráfica 1: Resultados Montecarlo (Scatter plot)
    ax1.clear()
    ax1.set_title(f"Resultados Simulación (Escenarios: {len(datos_y)})")
    if datos_x:
        ax1.scatter(datos_x, datos_y, c='blue', alpha=0.5)

    # Gráfica 2: Rendimiento de Clientes (Bar chart)
    ax2.clear()
    ax2.set_title(f"Carga de Trabajo por Nodo (Prod: {progreso_productor} items)")
    clientes = list(stats_clientes.keys())
    cantidad = list(stats_clientes.values())

    if clientes:
        ax2.bar(clientes, cantidad, color='orange')
        ax2.set_ylim(0, max(cantidad) + 5)


ani = animation.FuncAnimation(fig, update, interval=500, cache_frame_data=False)
plt.title("Simulación Montecarlo Distribuida")
plt.show()