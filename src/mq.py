import abc
import dataclasses
import enum
import json
import uuid
from typing import Any, Callable, Self


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


class MessageType(enum.StrEnum):
    ModelOne = "ModelOne"
    ModelTwo = "ModelTwo"


class MessageEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, uuid.UUID):
            return str(o)
        if isinstance(o, enum.Enum):
            return str(o.value)
        else:
            super().default(o)


@dataclasses.dataclass
class Message:
    id: uuid.UUID
    message_type: MessageType
    payload: dict[str, Any]
    priority: Priority
    delay: int = 0  # minutes
    attempts: int = 0
    max_attempts: int = 3

    def deserialise(self, pretty: bool = False) -> str:
        if pretty:
            return json.dumps(
                dataclasses.asdict(self),
                indent=4,
                sort_keys=True,
                cls=MessageEncoder,
            )
        else:
            return json.dumps(
                dataclasses.asdict(self),
                cls=MessageEncoder,
            )

    @staticmethod
    def serialise(message: str) -> Self:
        parsed = json.loads(message)
        return Message(**parsed)


class MessageQueue(abc.ABC):
    @abc.abstractmethod
    def publish(self, messages: list[Message]) -> list[uuid.UUID]:
        pass

    @abc.abstractmethod
    def consume(self, n: int) -> list[Message]:
        pass

    @abc.abstractmethod
    def consume_by_id(self, ids: list[uuid.UUID]) -> list[Message]:
        pass

    @abc.abstractmethod
    def retry(self, n: int) -> list[Message]:
        pass

    @abc.abstractmethod
    def retry_by_id(self, ids: list[uuid.UUID]) -> list[Message]:
        pass

    @abc.abstractmethod
    def retry_dlq(self, n: int) -> list[Message]:
        pass

    @abc.abstractmethod
    def statuses(self, ids: list[uuid.UUID]) -> list[tuple[uuid.UUID, Status]]:
        pass

    @abc.abstractmethod
    def dlq(self, n: int) -> list[Message]:
        pass

    @abc.abstractmethod
    async def execute(self, message: Message, handler: Callable) -> None:
        pass

    @abc.abstractmethod
    def complete(self, id: uuid.UUID) -> None:
        pass
