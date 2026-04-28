import json
import os
import time

import requests
from kafka import KafkaProducer
from sseclient import SSEClient


STREAM_URL = os.getenv(
    "STREAM_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "wikimedia.recentchange")

USER_AGENT = os.getenv(
    "USER_AGENT",
    "ProyectoFinal-Eqp1/1.0 (contacto: equipo1@itam.mx)",
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/event-stream",
    "Cache-Control": "no-cache",
}


def create_producer(max_retries=10, wait_seconds=5):
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
            print(f"[KAFKA] Productor conectado a {KAFKA_HOST}:{KAFKA_PORT}")
            return producer
        except Exception as exc:
            print(
                f"[KAFKA] Intento {attempt}/{max_retries} falló: {exc}. "
                f"Reintentando en {wait_seconds} segundos..."
            )
            time.sleep(wait_seconds)

    raise ConnectionError("No fue posible conectarse a Kafka.")


def stream_events():
    print(f"[WIKIMEDIA] Conectando a {STREAM_URL} ...")
    print(f"[WIKIMEDIA] User-Agent usado: {USER_AGENT}")

    response = requests.get(
        STREAM_URL,
        stream=True,
        timeout=60,
        headers=HEADERS,
    )
    response.raise_for_status()
    return SSEClient(response)


def main():
    producer = None

    try:
        producer = create_producer()
        client = stream_events()

        print("[PIPELINE] Iniciando flujo Wikimedia → Kafka...")

        for event in client.events():
            if not event.data:
                continue

            try:
                payload = json.loads(event.data)

                if not isinstance(payload, dict):
                    continue

                if payload.get("meta", {}).get("domain") == "canary":
                    continue

                producer.send(TOPIC_NAME, payload)
                producer.flush()

                print(
                    f"[SEND] Evento enviado | "
                    f"title={payload.get('title')!r} | "
                    f"user={payload.get('user')!r} | "
                    f"type={payload.get('type')!r}"
                )

            except json.JSONDecodeError:
                print("[WARN] Evento recibido no es JSON válido.")
            except Exception as exc:
                print(f"[ERROR] No se pudo enviar el evento a Kafka: {exc}")

    except KeyboardInterrupt:
        print("\n[PIPELINE] Proceso detenido manualmente.")
    except Exception as exc:
        print(f"[ERROR] Ocurrió un error en Wikimedia → Kafka: {type(exc).__name__}: {exc}")
    finally:
        if producer is not None:
            producer.close()
            print("[KAFKA] Productor cerrado.")


if __name__ == "__main__":
    main()