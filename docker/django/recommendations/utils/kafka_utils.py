import json
from kafka import KafkaProducer

# Configuración del broker Kafka
KAFKA_BROKER = 'broker:29092'  # Si estás dentro de docker-compose
TOPIC_NAME = 'music-recommendation-features'

# Lista de campos requeridos en el payload
REQUIRED_FIELDS = ['tempo', 'loudness', 'duration', 'key', 'mode', 'time_signature']

# Inicializamos el productor de forma perezosa (lazy initialization)
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
    missing_fields = [field for field in REQUIRED_FIELDS if field not in features]
    if missing_fields:
        raise InvalidPayloadError(f"Missing fields in features: {missing_fields}")

def publish_song_features(features: dict):
    """
    Publica el diccionario de features a Kafka tras validarlo.
    """
    try:
        # Validación previa
        validate_features(features)

        # Inicialización lazy del productor
        producer = get_kafka_producer()

        # Envío del mensaje
        producer.send(TOPIC_NAME, features)
        producer.flush()

        print("Features enviados correctamente a Kafka.")

    except InvalidPayloadError as e:
        print(f"Error de validación: {e}")
        raise

    except Exception as e:
        print(f"Error enviando a Kafka: {e}")
        raise
