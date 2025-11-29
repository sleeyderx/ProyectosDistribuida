import pika
import json
import numpy as np
import sys
import time
import uuid
from datetime import datetime


def leer_modelo_txt(path):
    variables = {}
    modelo = None
    num_simulaciones = None

    try:
        with open(path, "r", encoding="utf-8") as f:
            lineas = f.readlines()
    except FileNotFoundError:
        print(f"ERROR: No se encontró el archivo '{path}'.")
        exit(1)

    for linea in lineas:
        linea = linea.strip()
        if not linea:
            continue
        if "escenarios" in linea.lower() or "simulaciones" in linea.lower():
            if "=" in linea:
                try:
                    num_simulaciones = int(linea.split("=")[1].strip())
                    print(f"Número de escenarios: {num_simulaciones}")
                except:
                    print("ERROR: Formato incorrecto para número de simulaciones")
            continue
        if "=" in linea and not linea.startswith("#"):
            partes = linea.split("=")
            if len(partes) == 2:
                nombre = partes[0].strip()
                valor = partes[1].strip()
                partes_valor = valor.split()
                if len(partes_valor) >= 2:
                    dist = partes_valor[0].lower()
                    try:
                        params = list(map(float, partes_valor[1:]))
                        variables[nombre] = (dist, params)
                        print(f"Variable registrada: {nombre} ~ {dist}{params}")
                    except:
                        print(f"ERROR: Parámetros inválidos en: {linea}")
                continue
        if not linea.startswith("#") and "=" not in linea:
            modelo = linea.replace("^", "**")
            print(f"Expresión del modelo encontrada: {modelo}")
            break

    if modelo is None:
        print("ERROR: No se encontró la expresión del modelo.")
        exit(1)

    if num_simulaciones is None:
        print("ADVERTENCIA: No se indicó número de simulaciones, usando 10000 por defecto.")
        num_simulaciones = 10000

    return variables, modelo, num_simulaciones


def limpiar_colas_existentes(host):
    """Eliminar las colas existentes para evitar conflictos"""
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        channel = connection.channel()

        # Eliminar colas existentes
        for queue in ['modelo', 'escenarios', 'resultados']:
            try:
                channel.queue_delete(queue=queue)
                print(f"Cola '{queue}' eliminada")
            except Exception as e:
                print(f"No se pudo eliminar cola '{queue}': {e}")

        connection.close()
        return True
    except Exception as e:
        print(f"Error limpiando colas: {e}")
        return False


def conectar_rabbitmq(host):
    """Conectar a RabbitMQ de forma simple SIN parámetros conflictivos"""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()

        # Declarar colas  adicionales para evitar conflictos
        channel.queue_declare(queue='modelo', durable=True)
        channel.queue_declare(queue='escenarios', durable=True)
        channel.queue_declare(queue='resultados', durable=True)

        print(f"Conectado a RabbitMQ en {host}")
        return connection, channel
    except Exception as e:
        print(f"Error conectando a RabbitMQ: {e}")
        return None, None


def generar_valor_individual(dist, params):
    dist = dist.lower()

    if dist == "normal":
        mu, sigma = params
        return np.random.normal(mu, sigma)
    elif dist == "uniform":
        low, high = params
        return np.random.uniform(low, high)
    elif dist == "poisson":
        lam = params[0]
        return np.random.poisson(lam)
    elif dist in ("exp", "exponential"):
        lam = params[0]
        return np.random.exponential(1 / lam)
    elif dist == "triangular":
        low, mode, high = params
        return np.random.triangular(low, mode, high)
    elif dist == "lognormal":
        mu, sigma = params
        return np.random.lognormal(mu, sigma)
    else:
        raise ValueError(f"Distribución no soportada: {dist}")


def main():
    if len(sys.argv) < 2:
        print("Uso: python productor.py modelo.txt [host_rabbitmq]")
        sys.exit(1)

    archivo_modelo = sys.argv[1]
    host_rabbitmq = sys.argv[2] if len(sys.argv) > 2 else "localhost"

    print("PRODUCTOR - SISTEMA MONTECARLO DISTRIBUIDO")
    print("Este productor permite que consumidores se unan en cualquier momento")

    # PRIMERO limpiar colas existentes
    print(f"Limpiando colas existentes en: {host_rabbitmq}")
    if not limpiar_colas_existentes(host_rabbitmq):
        print("Advertencia: No se pudieron limpiar todas las colas")

    # Leer modelo
    print(f"Leyendo modelo desde: {archivo_modelo}")
    variables, modelo_expr, total_simulaciones = leer_modelo_txt(archivo_modelo)

    # Conectar a RabbitMQ
    connection, channel = conectar_rabbitmq(host_rabbitmq)
    if not connection:
        sys.exit(1)

    try:
        # 1. Publicar el modelo
        modelo_id = str(uuid.uuid4())
        modelo_msg = {
            'modelo_id': modelo_id,
            'timestamp': datetime.now().isoformat(),
            'variables': variables,
            'expresion': modelo_expr,
            'total_escenarios': total_simulaciones,
            'tipo': 'modelo'
        }

        print(f"Publicando modelo (ID: {modelo_id})")
        for i in range(5):  # Copias para nuevos consumidores
            channel.basic_publish(
                exchange='',
                routing_key='modelo',
                body=json.dumps(modelo_msg),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            time.sleep(0.5)

        print(f"Modelo publicado: {modelo_expr}")
        print(f"Variables: {list(variables.keys())}")
        print(f"Escenarios: {total_simulaciones}")

        # 2. Generar y publicar escenarios
        print(f"Generando {total_simulaciones} escenarios...")

        def crear_escenario(escenario_id):
            datos_vars = {}
            for nombre, (dist, params) in variables.items():
                datos_vars[nombre] = generar_valor_individual(dist, params)

            return {
                'escenario_id': escenario_id,
                'modelo_id': modelo_id,
                'variables': datos_vars,
                'timestamp': datetime.now().isoformat()
            }

        # Enviar escenarios en lotes pequeños para mejor distribución
        inicio_escenarios = time.time()
        lote_size = 50

        for lote_inicio in range(0, total_simulaciones, lote_size):
            lote_fin = min(lote_inicio + lote_size, total_simulaciones)

            for i in range(lote_inicio, lote_fin):
                escenario = crear_escenario(i)
                channel.basic_publish(
                    exchange='',
                    routing_key='escenarios',
                    body=json.dumps(escenario),
                    properties=pika.BasicProperties(delivery_mode=2)
                )

            # Mostrar progreso
            porcentaje = (lote_fin / total_simulaciones) * 100
            print(f"Enviados {lote_fin}/{total_simulaciones} escenarios ({porcentaje:.1f}%)")

            # Pequeña pausa entre lotes para permitir balanceo
            if lote_fin < total_simulaciones:
                time.sleep(0.1)

        tiempo_escenarios = time.time() - inicio_escenarios
        print(f"Todos los escenarios enviados en {tiempo_escenarios:.2f} segundos")

        # 3. Enviar mensaje de finalización
        fin_msg = {
            'tipo': 'fin_escenarios',
            'modelo_id': modelo_id,
            'total_escenarios': total_simulaciones,
            'timestamp': datetime.now().isoformat()
        }

        print("Enviando mensajes de finalización...")
        for i in range(3):
            channel.basic_publish(
                exchange='',
                routing_key='escenarios',
                body=json.dumps(fin_msg),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            time.sleep(1)

        print("Simulación completada.")

        print("Presiona Ctrl+C para finalizar")

        # Reenviar modelo para nuevos consumidores
        while True:
            channel.basic_publish(
                exchange='',
                routing_key='modelo',
                body=json.dumps(modelo_msg),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            time.sleep(10)

    except KeyboardInterrupt:
        print("\nProductor finalizado por el usuario")
    except Exception as e:
        print(f"Error durante el envío: {e}")
    finally:
        connection.close()
        print("Conexión cerrada")


if __name__ == "__main__":
    main()