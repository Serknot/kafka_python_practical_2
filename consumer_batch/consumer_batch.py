"""
BatchMessageConsumer — читает МИНИМУМ по 10 сообщений за один poll/consume,
обрабатывает их в цикле и коммитит оффсет один раз после обработки всей пачки
(ручной коммит, enable.auto.commit=False).

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
logger = logging.getLogger("batch_consumer")

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "my-topic")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
POLL_TIMEOUT_SECONDS = float(os.environ.get("POLL_TIMEOUT_SECONDS", "5.0"))

CONSUMER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "batch-consumer-group",     # уникальный group_id для этого типа консьюмера
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,             # коммитим оффсет вручную, один раз на пачку
    # Ждём накопления хотя бы ~1KB данных на брокере перед ответом консьюмеру,
    # чтобы за один fetch-запрос прилетало сразу много сообщений.
    "fetch.min.bytes": 1024,
    # Но не дольше 2 секунд — если сообщений мало, всё равно получим то, что есть.
    "fetch.max.wait.ms": 2000,
}


def run():
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([TOPIC])
    logger.info(
        "BatchMessageConsumer запущен, group_id=%s, batch_size>=%s",
        CONSUMER_CONFIG["group.id"], BATCH_SIZE,
    )

    try:
        while True:
            # consume() — батчевый аналог poll(): вернёт до BATCH_SIZE
            # сообщений, дождавшись их согласно fetch.min.bytes/fetch.max.wait.ms.
            batch = consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_SECONDS)

            if not batch:
                continue

            logger.info("Получена пачка из %s сообщений", len(batch))

            processed_any = False
            for msg in batch:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Ошибка консьюмера: %s", msg.error())
                    continue

                try:
                    message = Message.deserialize(msg.value())
                except Exception as exc:
                    print(f"[ОШИБКА ДЕСЕРИАЛИЗАЦИИ] {exc}")
                    logger.exception(
                        "Не удалось десериализовать сообщение offset=%s", msg.offset()
                    )
                    continue

                print(
                    f"[BATCH-CONSUMER] Получено: partition={msg.partition()} "
                    f"offset={msg.offset()} message={message}"
                )

                try:
                    process_message(message)
                    processed_any = True
                except Exception:
                    logger.exception(
                        "Ошибка обработки сообщения offset=%s, продолжаем пачку", msg.offset()
                    )

            if processed_any:
                try:
                    # Один синхронный коммит на всю пачку сразу после обработки.
                    consumer.commit(asynchronous=False)
                    logger.info("Оффсет закоммичен после обработки пачки из %s сообщений", len(batch))
                except KafkaException:
                    logger.exception("Не удалось закоммитить оффсет после пачки")

    except KeyboardInterrupt:
        logger.info("Остановка BatchMessageConsumer по запросу пользователя...")
    except KafkaException:
        logger.exception("Фатальная ошибка Kafka-клиента")
    finally:
        consumer.close()
        logger.info("BatchMessageConsumer остановлен.")


def process_message(message: Message) -> None:
    """Заглушка бизнес-логики обработки одного сообщения из пачки."""
    pass


if __name__ == "__main__":
    run()
