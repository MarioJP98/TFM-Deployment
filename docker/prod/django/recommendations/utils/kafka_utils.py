import json
import time
from kafka import KafkaProducer, KafkaConsumer
import hashlib

# Configuración del broker Kafka
KAFKA_BROKER = 'broker:29092'  # Si estás dentro de docker-compose
TOPIC_NAME = 'music-recommendation-features'
RESULT_TOPIC = 'music-recommendation-result'

# Campos requeridos por el modelo (ya actualizados)
REQUIRED_FIELDS = ['tempo', 'loudness', 'key', 'mode', 'time_signature']

# Productor Kafka global (lazy)
_producer = None


class InvalidPayloadError(Exception):
    """Excepción personalizada para payloads inválidos"""
    pass

def get_kafka_producer():
    """
    Inicializa el KafkaProducer solo cuando se necesite.
    """
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    return _producer

def validate_features(features: dict):
    """
    Valida que el diccionario de features contiene los campos requeridos.
    """
    missing_fields = [
        field for field in REQUIRED_FIELDS if field not in features]
    if missing_fields:
        raise InvalidPayloadError(
            f"Missing fields in features: {missing_fields}")


def generate_song_id(track_name, artist_name):
    """
    Genera un ID único hash a partir del título y artista.
    """
    combined_str = f"{track_name}-{artist_name}"
    sha1_hash = hashlib.sha1(combined_str.encode('utf-8')).hexdigest()
    song_id = sha1_hash[:16].upper()
    return song_id


def publish_song_features(features: dict):
    """
    Publica el diccionario de features a Kafka tras validarlo.
    """
    try:
        validate_features(features)

        # Forzar tipos correctos
        features["tempo"] = float(features.get("tempo", 0.0))
        features["loudness"] = float(features.get("loudness", -60.0))
        features["key"] = int(features.get("key", 0))
        features["mode"] = int(features.get("mode", 1))
        features["time_signature"] = int(features.get("time_signature", 4))

        producer = get_kafka_producer()

        recommendation_id = generate_song_id(
            features["track_name"],
            features["artist_name"]
        ).encode('utf-8')

        producer.send(
            TOPIC_NAME,
            key=recommendation_id,
            value=features
        )
        producer.flush()

        print("✅ Features enviados correctamente a Kafka.")
        return recommendation_id

    except InvalidPayloadError as e:
        print(f"❌ Error de validación: {e}")
        raise

    except Exception as e:
        print(f"❌ Error enviando a Kafka: {e}")
        raise


def consume_recommendations_for_id(recommendation_id, timeout=10):
    """
    Escucha el topic de recomendaciones por un tiempo limitado y filtra por recommendation_id.
    """
    consumer = KafkaConsumer(
        RESULT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=None,
        consumer_timeout_ms=timeout * 1000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )

    result = []
    start_time = time.time()

    for msg in consumer:
        if msg.key == recommendation_id:
            result.append(msg.value)

        if len(result) >= 3 or (time.time() - start_time) > timeout:
            break

    consumer.close()
    return result
