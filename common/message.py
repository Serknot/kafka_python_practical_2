"""
Общий класс сообщения для продюсера и консьюмеров.

"""

import json
import time
import uuid
from dataclasses import dataclass, asdict


@dataclass
class Message:
    """Модель сообщения, которое летает через Kafka."""

    message_id: str      # уникальный идентификатор сообщения (для трассировки)
    timestamp: float      # unix-время создания сообщения
    sequence: int         # порядковый номер сообщения от продюсера
    content: str           # полезная нагрузка

    @staticmethod
    def create(sequence: int, content: str) -> "Message":
        return Message(
            message_id=str(uuid.uuid4()),
            timestamp=time.time(),
            sequence=sequence,
            content=content,
        )

    def serialize(self) -> bytes:
        """
        Сериализация сообщения в JSON -> bytes.
        Бросает исключение наружу, чтобы вызывающий код мог залогировать
        ошибку по месту использования (там, где есть контекст: продюсер/консьюмер).
        """
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")

    @staticmethod
    def deserialize(raw: bytes) -> "Message":
        """Десериализация bytes -> Message. Бросает исключение при некорректных данных."""
        data = json.loads(raw.decode("utf-8"))
        return Message(**data)
