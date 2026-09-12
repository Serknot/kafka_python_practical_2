"""
Общие вспомогательные функции для продюсера и консьюмеров, чтобы не
дублировать настройку логирования и обработку служебных ошибок Kafka
в каждом из трёх приложений.
"""

import logging

from confluent_kafka import KafkaError


def configure_logging(name: str) -> logging.Logger:
    """
    Единая настройка логирования для продюсера/консьюмеров.
    Вызывается один раз при старте каждого приложения.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return logging.getLogger(name)


def is_partition_eof(kafka_error: KafkaError) -> bool:
    """
    True, если ошибка сообщения — это просто отметка о том, что консьюмер
    дочитал до конца партиции на данный момент (это не настоящая ошибка
    и не повод логировать её как проблему).
    """
    return kafka_error is not None and kafka_error.code() == KafkaError._PARTITION_EOF
