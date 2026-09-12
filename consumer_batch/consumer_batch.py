"""
BatchMessageConsumer — читает МИНИМУМ по 10 сообщений за один poll/consume,
обрабатывает их в цикле и коммитит оффсет ОДИН РАЗ после обработки всей пачки
(ручной коммит, enable.auto.commit=False).

"""

import os
import sys

from confluent_kafka import Consumer, KafkaException

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from common.message import Message  # noqa: E402
from common.kafka_utils import configure_logging, is_partition_eof  # noqa: E402

logger = configure_logging("batch_consumer")

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

            has_records = False

            for msg in batch:
                if msg.error():
                    if is_partition_eof(msg.error()):
                        continue
                    logger.error("Ошибка консьюмера: %s", msg.error())
                    continue

                has_records = True

                try:
                    message = Message.deserialize(msg.value())
                except Exception as exc:
                    print(f"[ОШИБКА ДЕСЕРИАЛИЗАЦИИ] {exc}")
                    logger.exception(
                        "Не удалось десериализовать сообщение offset=%s "
                        "(будет пропущено, оффсет всё равно продвинется)",
                        msg.offset(),
                    )
                    continue

                print(
                    f"[BATCH-CONSUMER] Получено: partition={msg.partition()} "
                    f"offset={msg.offset()} message={message}"
                )

                try:
                    process_message(message)
                except Exception:
                    logger.exception(
                        "Ошибка обработки сообщения offset=%s (сообщение считается "
                        "обработанным — залогировано, оффсет продвинется), "
                        "продолжаем пачку",
                        msg.offset(),
                    )

            if has_records:
                try:
                    # Один синхронный коммит на всю пачку сразу после обработки
                    # (в том числе после обработки битых сообщений — см. комментарий выше).
                    consumer.commit(asynchronous=False)
                    logger.info(
                        "Оффсет закоммичен после обработки пачки из %s сообщений", len(batch)
                    )
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
