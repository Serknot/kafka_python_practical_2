"""
Продюсер сообщений в Kafka-топик (модель push).
Гарантия доставки: At Least Once.
"""

import json
import logging
import os
import sys
import time

from confluent_kafka import Producer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from common.message import Message  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("producer")

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "my-topic")
SEND_INTERVAL_SECONDS = float(os.environ.get("SEND_INTERVAL_SECONDS", "1"))

PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",          # At Least Once: ждём подтверждение от всех ISR-реплик
    "retries": 5,            # число повторных попыток отправки при ошибке
    "retry.backoff.ms": 500,  # пауза между повторными попытками
    "linger.ms": 100,        # немного группируем сообщения перед отправкой
    "client.id": "single-producer",
}


def delivery_report(err, msg):
    """
    Callback, который вызывается librdkafka асинхронно после того, как
    брокер подтвердил (или не подтвердил) получение сообщения.
    """
    if err is not None:
        logger.error("Ошибка доставки сообщения в партицию %s: %s", msg.partition(), err)
    else:
        logger.info(
            "Сообщение доставлено: topic=%s partition=%s offset=%s",
            msg.topic(), msg.partition(), msg.offset(),
        )


def run():
    producer = Producer(PRODUCER_CONFIG)
    sequence = 0

    logger.info("Продюсер запущен. Отправка сообщений в топик '%s'...", TOPIC)

    try:
        while True:
            sequence += 1
            message = Message.create(sequence=sequence, content=f"event-{sequence}")

            try:
                payload = message.serialize()
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                # Проблема сериализации — логируем и на консоль, и в лог,
                # само сообщение при этом не отправляем, но продюсер продолжает работать.
                print(f"[ОШИБКА СЕРИАЛИЗАЦИИ] {exc}")
                logger.exception("Не удалось сериализовать сообщение #%s", sequence)
                continue

            print(f"[PRODUCER] Отправка: {payload.decode('utf-8')}")

            try:
                # produce() асинхронен: сообщение кладётся в очередь,
                # а doclivery_report вызовется позже, когда poll()/flush()
                # обработает события librdkafka.
                producer.produce(
                    topic=TOPIC,
                    key=str(message.message_id).encode("utf-8"),
                    value=payload,
                    callback=delivery_report,
                )
            except BufferError:
                logger.warning("Локальная очередь продюсера переполнена, ждём и повторяем...")
                producer.poll(1)
                continue
            except Exception:
                logger.exception("Непредвиденная ошибка при отправке сообщения #%s", sequence)
                continue

            # poll(0) обрабатывает накопившиеся callback'и доставки без блокировки
            producer.poll(0)
            time.sleep(SEND_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Остановка продюсера по запросу пользователя...")
    finally:
        # flush() — аналог close() для io-потока: гарантированно дожидаемся
        # отправки всех сообщений, оставшихся в буфере, перед выходом.
        logger.info("Дожидаемся отправки оставшихся сообщений (flush)...")
        producer.flush(timeout=10)
        logger.info("Продюсер остановлен.")


if __name__ == "__main__":
    run()
