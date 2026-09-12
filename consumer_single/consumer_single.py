"""
SingleMessageConsumer — читает по одному сообщению за вызов poll(),
обрабатывает его и коммитит оффсет АВТОМАТИЧЕСКИ (enable.auto.commit=True).

"""

import logging
import os
import sys

from confluent_kafka import Consumer, KafkaException, KafkaError

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from common.message import Message  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("single_consumer")

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "my-topic")

CONSUMER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "single-consumer-group",   # уникальный group_id для этого типа консьюмера
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,             # автоматический коммит оффсета
    "auto.commit.interval.ms": 1000,        # как часто фоново коммитится оффсет
}


def run():
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([TOPIC])
    logger.info("SingleMessageConsumer запущен, group_id=%s", CONSUMER_CONFIG["group.id"])

    try:
        while True:
            # Читаем ровно ОДНО сообщение за раз (модель pull).
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # это не ошибка, а просто конец партиции на данный момент
                    continue
                logger.error("Ошибка консьюмера: %s", msg.error())
                continue

            try:
                message = Message.deserialize(msg.value())
            except Exception as exc:
                print(f"[ОШИБКА ДЕСЕРИАЛИЗАЦИИ] {exc}")
                logger.exception("Не удалось десериализовать сообщение offset=%s", msg.offset())
                # оффсет всё равно закоммитится автоматически, приложение продолжает работать
                continue

            print(
                f"[SINGLE-CONSUMER] Получено: partition={msg.partition()} "
                f"offset={msg.offset()} message={message}"
            )

            try:
                # Здесь могла бы быть бизнес-логика обработки сообщения
                process_message(message)
            except Exception:
                logger.exception("Ошибка обработки сообщения offset=%s, продолжаем работу", msg.offset())

    except KeyboardInterrupt:
        logger.info("Остановка SingleMessageConsumer по запросу пользователя...")
    except KafkaException:
        logger.exception("Фатальная ошибка Kafka-клиента")
    finally:
        # close() — аналог закрытия io-потока: сообщаем группе, что уходим,
        # чтобы не ждать session timeout при ребалансировке.
        consumer.close()
        logger.info("SingleMessageConsumer остановлен.")


def process_message(message: Message) -> None:
    """Заглушка бизнес-логики обработки одного сообщения."""
    pass


if __name__ == "__main__":
    run()
