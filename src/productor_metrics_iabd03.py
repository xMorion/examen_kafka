import asyncio
from confluent_kafka import Producer
import random
import time
from datetime import datetime, timezone
import uuid
import json 

# --- Configuración ---
SERVER_IDS = ["web01", "web02", "db01", "app01", "cache01"]
REPORTING_INTERVAL_SECONDS = 10  # Tiempo entre reportes completos de todos los servers

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = "10.32.24.131:29092"
KAFKA_TOPIC = "system-metrics-topic-iabd03"

# Crea el productor
PRODUCER = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

# --- Inicio de la Lógica de Generación (Parte A, Paso 2) ---
if __name__ == "__main__":
    print("Iniciando simulación de generación de métricas...")
    print(f"Servidores simulados: {SERVER_IDS}")
    print(f"Intervalo de reporte: {REPORTING_INTERVAL_SECONDS} segundos")
    print("-" * 30)

    try:
        while True:
            print(f"\n{datetime.now()}: Generando reporte de métricas...")
            # Iterar sobre cada servidor para generar sus métricas
            for server_id in SERVER_IDS:
                # Generar métricas simuladas con fluctuaciones
                cpu_percent = random.uniform(5.0, 75.0)
                # Añadir un pico ocasional de CPU
                if random.random() < 0.1:  # 10% de probabilidad
                    cpu_percent = random.uniform(85.0, 98.0)

                memory_percent = random.uniform(20.0, 85.0)
                # Añadir un pico ocasional de memoria
                if random.random() < 0.05:  # 5% de probabilidad
                    memory_percent = random.uniform(90.0, 99.0)

                disk_io_mbps = random.uniform(0.1, 50.0)
                network_mbps = random.uniform(1.0, 100.0)

                # Errores deben ser poco frecuentes
                error_count = 0
                if random.random() < 0.08:  # 8% probabilidad de tener algún error
                    error_count = random.randint(1, 3)

                # Crear el diccionario del mensaje de métricas
                metric_message = {
                    "server_id": server_id,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),  # Usar timezone.utc
                    "metrics": {
                        "cpu_percent": round(cpu_percent, 2),
                        "memory_percent": round(memory_percent, 2),
                        "disk_io_mbps": round(disk_io_mbps, 2),
                        "network_mbps": round(network_mbps, 2),
                        "error_count": error_count
                    },
                    "message_uuid": str(uuid.uuid4())  # Identificador único del mensaje
                }

                # --- Punto de Integración ---
                # Aquí es donde, en el script completo, enviarías `metric_message` a Kafka.
                # Por ahora, solo lo imprimimos para ver qué se genera.
                print(f" Generado para {server_id}:")
                # Imprimir de forma legible (opcional)
                print(f"  CPU: {metric_message['metrics']['cpu_percent']}%")
                print(f"  Mem: {metric_message['metrics']['memory_percent']}%")
                print(f"  Disk: {metric_message['metrics']['disk_io_mbps']} MB/s")
                print(f"  Net: {metric_message['metrics']['network_mbps']} Mbps")
                print(f"  Errors: {metric_message['metrics']['error_count']}")
                # O imprimir el JSON completo (más útil para ver la estructura final)
                # print(json.dumps(metric_message, indent=2))
                # print("-" * 10)

            # Envía el mensaje a Kafka
            PRODUCER.produce(KAFKA_TOPIC, json.dumps(metric_message).encode("utf-8"))
            PRODUCER.flush()

            # Esperar antes de generar el siguiente reporte completo
            print(f"\nReporte completo generado. Esperando {REPORTING_INTERVAL_SECONDS} segundos...")
            time.sleep(REPORTING_INTERVAL_SECONDS)

            
    except KeyboardInterrupt:
        print("\nSimulación detenida por el usuario.")
    finally:
        PRODUCER.close()
        print("Conexión con Kafka cerrada.")
# --- Fin de la Lógica de Generación ---
