# Kafka Metrics Consumer/Producer

Este proyecto simula la recolección de métricas de sistema, las envía a un topic de Kafka y un consumidor las inserta en MongoDB Atlas, calculando KPIs cada 20 mensajes.

## Estructura

- `src/`: contiene el productor y consumidor
- `requirements.txt`: dependencias necesarias

## Explicacion del codigo

Este programa genera una prueba de escritura/lectura en topicos de kafka dentro de una maquina docker.

El producer genera un topico (si no existe) y guarda unos datos ficticios cada 10 segundos en un topico de kafka.
Mientras que el consumer recoge los datos del topico y los escribe en una BD de MongoDB. A su vez, el consumer tambien guarda unos KPI cada vesz que recibe 20 mensajes que guarda en una coleccion distinta de MongoDB a la que guarda los datos en bruto

## Uso

1. Ejecuta el productor:
```bash
python src/productor_metrics_iabd03.py
```
2. Ejecusta el consumer:
```bash
python src/consumidor_metrics_iabd03
```
