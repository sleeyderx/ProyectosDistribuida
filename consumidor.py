import pika
import json
import math
import sys
import time
import random  # <--- Necesario para IDs únicos
import numpy as np  # <--- Necesario para estadísticas


class Consumidor:
    def __init__(self, host_rabbitmq="localhost", consumidor_id=None):
        self.host_rabbitmq = host_rabbitmq

        # --- CORRECCIÓN 1: ID ÚNICO ---
        # Agregamos random para que si lanzas varios workers al mismo tiempo,
        # el visualizador los distinga como workers diferentes.
        suffix = random.randint(1000, 9999)
        self.consumidor_id = consumidor_id or f"worker_{int(time.time())}_{suffix}"

        self.modelo_actual = None
        self.modelo_id = None
        self.connection = None
        self.channel = None
        self.resultados = []

        # Variables para controlar qué estamos escuchando
        self.tag_modelo = None
        self.tag_escenarios = None

        print(f"Iniciando Worker: {self.consumidor_id}")

    def conectar_rabbitmq(self):
        try:
            print(f"Conectando a RabbitMQ en {self.host_rabbitmq}...")
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host_rabbitmq, heartbeat=600)
            )
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=50)  # Aumentamos un poco el prefetch para eficiencia

            # Declarar colas
            self.channel.queue_declare(queue='modelo', durable=True)
            self.channel.queue_declare(queue='escenarios', durable=True)
            self.channel.queue_declare(queue='resultados', durable=True)
            return True
        except Exception as e:
            print(f"Error conectando a RabbitMQ: {e}")
            return False

    def callback_modelo(self, ch, method, properties, body):
        """Recibe la fórmula matemática"""
        try:
            modelo_msg = json.loads(body.decode('utf-8'))
            if modelo_msg.get('tipo') == 'modelo':
                self.modelo_actual = modelo_msg
                self.modelo_id = modelo_msg['modelo_id']
                self.resultados = []  # Limpiar memoria anterior

                print(f"Modelo recibido: {modelo_msg['expresion']}")
                ch.basic_ack(delivery_tag=method.delivery_tag)

                # --- CORRECCIÓN 2: FLUJO DE ESTADOS ---
                # Dejamos de escuchar 'modelo' para enfocarnos en 'escenarios'
                if self.tag_modelo:
                    self.channel.basic_cancel(self.tag_modelo)
                    self.tag_modelo = None

                self.iniciar_consumo_escenarios()
            else:
                ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            print(f"Error modelo: {e}")

    def iniciar_consumo_escenarios(self):
        print("Esperando escenarios de trabajo...")
        self.tag_escenarios = self.channel.basic_consume(
            queue='escenarios',
            on_message_callback=self.callback_escenarios,
            auto_ack=False
        )

    def evaluar_modelo(self, expr, vars_dict):
        funcs = {"sin": math.sin, "cos": math.cos, "tan": math.tan,
                 "sqrt": math.sqrt, "exp": math.exp, "log": math.log,
                 "pi": math.pi, "e": math.e}
        try:
            return eval(expr, {"__builtins__": None, **funcs}, vars_dict)
        except Exception:
            return None

    def callback_escenarios(self, ch, method, properties, body):
        try:
            mensaje = json.loads(body.decode('utf-8'))

            # --- CORRECCIÓN 3: REPORTE FINAL ---
            if mensaje.get('tipo') == 'fin_escenarios':
                ch.basic_ack(delivery_tag=method.delivery_tag)

                print(f"\nRESULTADOS FINALES - {self.consumidor_id}")
                if len(self.resultados) > 0:
                    arr_final = np.array(self.resultados)
                    print(f"   Media final: {arr_final.mean():.6f}")
                    print(f"   Desviacion estandar: {arr_final.std():.6f}")
                    print(f"   Minimo: {arr_final.min():.6f}")
                    print(f"   Maximo: {arr_final.max():.6f}")
                    print(f"   Total procesados: {len(self.resultados)}")
                else:
                    print("   No se procesaron datos para este modelo.")

                # --- VOLVER AL ESTADO DE ESPERA ---
                # 1. Dejar de escuchar escenarios
                if self.tag_escenarios:
                    self.channel.basic_cancel(self.tag_escenarios)
                    self.tag_escenarios = None

                # 2. Volver a escuchar modelos
                print("Esperando nuevo modelo...")
                self.tag_modelo = self.channel.basic_consume(
                    queue='modelo',
                    on_message_callback=self.callback_modelo
                )
                return

            # Procesamiento normal
            if self.modelo_actual and mensaje.get('modelo_id') == self.modelo_id:
                resultado = self.evaluar_modelo(
                    self.modelo_actual['expresion'],
                    mensaje['variables']
                )

                if resultado is not None:
                    res_float = float(resultado)
                    self.resultados.append(res_float)

                    # Enviar al visualizador
                    resultado_msg = {
                        'tipo': 'resultado',
                        'worker_id': self.consumidor_id,
                        'modelo_id': self.modelo_id,
                        'resultado': res_float,
                        'timestamp': time.time()
                    }
                    self.channel.basic_publish(
                        exchange='',
                        routing_key='resultados',
                        body=json.dumps(resultado_msg),
                        properties=pika.BasicProperties(delivery_mode=1)
                        # Modo 1 (no persistente) es más rápido para resultados
                    )

                    if len(self.resultados) % 100 == 0:
                        print(f"Procesados: {len(self.resultados)}")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(f"Error procesando: {e}")
            # Intentar no perder el mensaje si fue un error transitorio,
            # pero aquí hacemos ack para no bloquear la cola.
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def iniciar(self):
        if self.conectar_rabbitmq():
            print("Buscando modelo...")
            self.tag_modelo = self.channel.basic_consume(
                queue='modelo',
                on_message_callback=self.callback_modelo
            )
            try:
                self.channel.start_consuming()
            except KeyboardInterrupt:
                print("Deteniendo worker...")
                self.channel.stop_consuming()
                self.connection.close()


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    Consumidor(host).iniciar()