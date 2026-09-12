"""
Продюсер сообщений в Kafka-топик (модель push).

Гарантия доставки: At Least Once ("как минимум один раз").
Это достигается комбинацией:
  - acks='all'            -> продюсер ждёт подтверждения от ВСЕХ ISR-реплик
                              брокера, прежде чем считать сообщение отправленным.
                              Это защищает от потери сообщения при падении лидера
                              партиции сразу после записи.
  - retries               -> при временной ошибке (например, недоступность
                              брокера) продюсер повторит отправку сам, а не
                              просто уронит сообщение.
  - enable.idempotence не включён (по умолчанию False) -> именно поэтому
                              гарантия остаётся "At Least Once", а не
                              "Exactly Once": при повторной отправке после
                              таймаута подтверждения теоретически возможна
                              дублирующая запись, но не потеря сообщения.

Хотя send()/produce() в Kafka асинхронны, работа с сокетом брокера — это
классический io-поток, поэтому в конце жизни продюсера обязательно
закрываем его (flush()/close() по аналогии с close() у файлов/сокетов),
чтобы не потерять сообщения, которые ещё лежат в буфере на отправку.
"""

import json
import os
import sys
import time

from confluent_kafka import Producer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from common.message import Message  # noqa: E402
from common.kafka_utils import configure_logging  # noqa: E402

logger = configure_logging("producer")

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


def send_with_retry(producer: Producer, message: Message, payload: bytes) -> None:
    """
    Отправляет ОДНО И ТО ЖЕ уже сериализованное сообщение, повторяя попытку
    при BufferError (локальная очередь librdkafka переполнена), пока
    сообщение не будет поставлено в очередь на отправку.

    Это дополняет server-side retries (настройка `retries` в PRODUCER_CONFIG,
    которая покрывает сетевые ошибки/недоступность брокера) client-side
    повтором именно для случая переполнения локального буфера — без этого
    `continue` в вызывающем цикле привёл бы к потере сообщения и нарушению
    гарантии At Least Once.
    """
    while True:
        try:
            # produce() асинхронен: сообщение кладётся в очередь,
            # а delivery_report вызовется позже, когда poll()/flush()
            # обработает события librdkafka.
            producer.produce(
                topic=TOPIC,
                key=str(message.message_id).encode("utf-8"),
                value=payload,
                callback=delivery_report,
            )
            return
        except BufferError:
            logger.warning(
                "Локальная очередь продюсера переполнена, ждём и повторяем "
                "отправку сообщения #%s (сообщение не отбрасывается)...",
                message.sequence,
            )
            # poll() освобождает место в очереди, обрабатывая уже
            # подтверждённые брокером callback'и, после чего повторяем
            # попытку отправить ТО ЖЕ САМОЕ сообщение.
            producer.poll(1)
        except Exception:
            logger.exception(
                "Непредвиденная ошибка при отправке сообщения #%s, повторяем попытку...",
                message.sequence,
            )
            time.sleep(0.5)


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

            # send_with_retry гарантирует, что ИМЕННО ЭТО сообщение будет
            # поставлено в очередь на отправку, прежде чем мы перейдём к
            # следующему. Если бы при BufferError мы просто делали `continue`,
            # sequence всё равно увеличился бы на следующей итерации, и это
            # сообщение оказалось бы потеряно — что противоречило бы
            # заявленной гарантии At Least Once.
            send_with_retry(producer, message, payload)

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
