import abc
from typing import Any, Optional

from src.mq import Message, MessageId, Status


class DatabaseConnector(abc.ABC):
    @abc.abstractmethod
    def _execute_query(
        self,
        template_name: str,
        params: Optional[dict[str, Any]] = None,
        network_timeout: int = 30,
        fetch: bool = False,
    ):
        pass

    @abc.abstractmethod
    def connection(self, network_timeout: int) -> Any:
        pass

    @abc.abstractmethod
    def initialise_mq(self, name: str):
        """Create Message Queue and Dead Letter Queue Tables."""
        pass

    @abc.abstractmethod
    def publish_messages(self, messages: list[Message]) -> list[MessageId]:
        pass

    @abc.abstractmethod
    def consume_messages(self, n: int) -> list[Message]:
        pass

    @abc.abstractmethod
    def consume_messages_by_id(self, ids: list[MessageId]) -> list[Message]:
        pass

    @abc.abstractmethod
    def message_statuses(self, ids: list[MessageId]) -> list[tuple[MessageId, Status]]:
        pass

    @abc.abstractmethod
    def retry_messages(self, ids: list[MessageId]) -> list[MessageId]:
        pass

    @abc.abstractmethod
    def fetch_dlq(self, n: int) -> list[Message]:
        pass

    @abc.abstractmethod
    def clean_mq(self) -> None:
        pass
