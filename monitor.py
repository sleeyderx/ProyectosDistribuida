import pika
import json
import numpy as np
import matplotlib.pyplot as plt
import threading
import sys
from threading import Lock
import time


class Visualizador:
    def __init__(self, host_rabbitmq="localhost"):
        self.host_rabbitmq = host_rabbitmq
        self.resultados = []
        self.stats_workers = {}
        self.lock = Lock()
        self.running = True

        plt.ion()
        self.fig = plt.figure(figsize=(14, 8))
        self.fig.canvas.manager.set_window_title('Monitor Distribuido')
        gs = self.fig.add_gridspec(2, 2)

        self.ax_hist = self.fig.add_subplot(gs[0, 0])
        self.ax_workers = self.fig.add_subplot(gs[0, 1])
        self.ax_media = self.fig.add_subplot(gs[1, :])
        plt.tight_layout(pad=4)

    def conectar_rabbitmq(self):
        while self.running:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host_rabbitmq, connection_attempts=5, retry_delay=2)
                )
                channel = connection.channel()
                channel.queue_declare(queue='resultados', durable=True)
                print("Monitor conectado...")

                def callback(ch, method, properties, body):
                    if not self.running:
                        channel.stop_consuming()
                        return
                    try:
                        msg = json.loads(body.decode('utf-8'))
                        if msg.get('tipo') == 'resultado':
                            val = float(msg['resultado'])
                            # Leer ID del worker para separar estadísticas
                            w_id = msg.get('worker_id', 'Anonimo')

                            with self.lock:
                                self.resultados.append(val)
                                self.stats_workers[w_id] = self.stats_workers.get(w_id, 0) + 1

                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    except:
                        pass

                channel.basic_consume(queue='resultados', on_message_callback=callback)
                channel.start_consuming()
            except:
                time.sleep(2)

    def actualizar_grafica(self):
        with self.lock:
            if not self.resultados: return
            data = np.array(self.resultados)
            workers = self.stats_workers.copy()

        # Histograma Global
        self.ax_hist.clear()
        self.ax_hist.hist(data, bins=40, color='#2c3e50', alpha=0.8)
        self.ax_hist.set_title(f'Distribución Global (N={len(data)})')
        self.ax_hist.grid(alpha=0.3)

        # Barras Consumidores
        self.ax_workers.clear()
        if workers:
            names = list(workers.keys())
            vals = list(workers.values())
            # Generar mapa de colores
            colors = plt.cm.viridis(np.linspace(0, 0.9, len(names)))

            bars = self.ax_workers.bar(names, vals, color=colors)
            self.ax_workers.tick_params(axis='x', rotation=45)
            self.ax_workers.set_title('Rendimiento por Nodo')

            # Texto encima de barras
            for rect in bars:
                height = rect.get_height()
                self.ax_workers.text(rect.get_x() + rect.get_width() / 2.0, height, f'{int(height)}', ha='center',
                                     va='bottom')


    def iniciar(self):
        t = threading.Thread(target=self.conectar_rabbitmq, daemon=True)
        t.start()
        print("Monitor iniciado.")
        try:
            while self.running:
                self.actualizar_grafica()
                plt.pause(0.5)
        except KeyboardInterrupt:
            self.running = False


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    Visualizador(host).iniciar()