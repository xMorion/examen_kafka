from confluent_kafka import Consumer
from pymongo import MongoClient
import json
import time
from datetime import datetime

# Configuraciones
KAFKA_BOOTSTRAP_SERVERS = "10.32.24.131:29092"  # Cambia por la IP de tu Kafka
KAFKA_TOPIC = "system-metrics-topic-iabd03"
GROUP_ID = "grupo_iabd03_id"
MONGO_URI = "mongodb+srv://iabd03:iabd03iabd03@cluster0.2dgzh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DB_NAME = "metrics_db"
RAW_COLLECTION = "system_metrics_raw_iabd03"
KPIS_COLLECTION = "system_metrics_kpis_iabd03"
BATCH_SIZE = 20

# Configuración del consumidor
config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest', # Para leer desde el principio - NO FUNCIONA - SOLO ES VALIDO CUANDO NO HAY UN OFFSET CREADO
    # 'enable.auto.commit': False  # Desactiva la confirmación automática del offset
}

# Crear el consumidor
consumer = Consumer(config)
consumer.subscribe([KAFKA_TOPIC])

# Conexión MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]
raw_col = db[RAW_COLLECTION]
kpi_col = db[KPIS_COLLECTION]

# Variables para KPI
buffer = []
window_size = 20
start_time = time.time()

try:
    consumer.subscribe([KAFKA_TOPIC])
    print("Esperando mensajes...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error en el mensaje:", msg.error())
            continue

        # Parseo del mensaje
        metric = json.loads(msg.value().decode('utf-8'))
        buffer.append(metric)

        # Traza de inicion de insercion
        print(f"Se ha recibido el siguiente mensaje del topcio: {metric}")
        # Inserta la métrica cruda
        raw_col.insert_one(metric)

        print("El mensaje se ha insertado correctamente en mongo")

        # Cuando se juntan 20 mensajes -> calcular KPIs
        if len(buffer) >= window_size:
            end_time = time.time()
            duration = end_time - start_time

            # Cálculo de KPIs
            avg_cpu = sum(m.get('cpu_percent', 0) for m in buffer) / window_size
            avg_mem = sum(m.get('memory_percent', 0) for m in buffer) / window_size
            avg_disk = sum(m.get('disk_io_mbps', 0) for m in buffer) / window_size
            avg_net = sum(m.get('network_mbps', 0) for m in buffer) / window_size
            sum_errors = sum(m.get('error_count', 0) for m in buffer)
            rate = window_size / duration if duration > 0 else 0

            kpis = {
                'timestamp': datetime.utcnow(),
                'window_duration_sec': duration,
                'num_messages': window_size,
                'avg_cpu_percent': avg_cpu,
                'avg_memory_percent': avg_mem,
                'avg_disk_io_mbps': avg_disk,
                'avg_network_mbps': avg_net,
                'total_error_count': sum_errors,
                'processing_rate_msgs_per_sec': rate
            }

            kpi_col.insert_one(kpis)
            print("KPIs calculados e insertados:", kpis)

            # Reiniciar buffer y temporizador
            buffer = []
            start_time = time.time()

except KeyboardInterrupt:
    print("Interrumpido por el usuario")

finally:
    consumer.close()
    client.close()
    print("Conexiones cerradas")
