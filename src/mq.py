import abc
import dataclasses
import enum
import json
import uuid
from typing import Any, Self


class Priority(enum.Enum):
    LOW = -1
    NORMAL = 0
    HIGH = 1
    IMMEDIATE = 99


class Status(enum.StrEnum):
    NEW = "NEW"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class MessageType(enum.Enum):
    ModelOne = 1
    ModelTwo = 2


type MessageId = uuid.UUID


@dataclasses.dataclass
class Message:
    id: MessageId
    message_type: enum.Enum
    data: dict[str, Any]
    priority: Priority
    delay: int = 0  # minutes
    attempts: int = 0
    max_attempts: int = 3

    def deserialise(self) -> str:
        return json.dumps(dataclasses.asdict(self))

    @staticmethod
    def serialise(message: str) -> Self:
        parsed = json.loads(message)
        return Message(**parsed)


class MessageQueue(abc.ABC):
    @abc.abstractmethod
    def publish(self, messages: list[Message]) -> list[MessageId]:
        pass

    @abc.abstractmethod
    def consume(self, n: int = 1) -> list[Message]:
        pass

    @abc.abstractmethod
    def consume_by_id(self, ids: list[MessageId]) -> list[Message]:
        pass

    @abc.abstractmethod
    def retry(self, ids: list[MessageId]) -> list[MessageId]:
        pass

    @abc.abstractmethod
    def statuses(self, ids: list[MessageId]) -> list[tuple[MessageId, Status]]:
        pass

    @abc.abstractmethod
    def dlq(self, n: int) -> list[Message]:
        pass
