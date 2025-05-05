# Kafka Metrics Consumer/Producer

Este proyecto simula la recolección de métricas de sistema, las envía a un topic de Kafka y un consumidor las inserta en MongoDB Atlas, calculando KPIs cada 20 mensajes.

## Estructura

- `src/`: contiene el productor y consumidor
- `requirements.txt`: dependencias necesarias

## Uso

1. Ejecuta el productor:
```bash
python src/productor_metrics_iabd03.py
```
2. Ejecusta el consumer:
```bash
python src/consumidor_metrics_iabd03
```
