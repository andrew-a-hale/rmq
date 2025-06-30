import os
import json
import uuid
from typing import Any, Optional

from snowflake import connector

from src.db import DatabaseConnector
from src.mq import Message, MessageId, MessageQueue, MessageType, Priority, Status

PATH = os.path.dirname(__file__)
DEFAULT_MESSAGE = Message(uuid.uuid4(), MessageType.ModelOne, {}, Priority.NORMAL)
DEFAULT_MESSAGE_ID = uuid.uuid4()


class Db(DatabaseConnector):
    def __init__(self, name: str, conn_params: dict[str, str]):
        self.name = name
        self.dlq = "dlq_" + name
        self.conn_params = conn_params
        self.initialise_mq(name=name)

    def _execute_query(
        self,
        template_name: str,
        params: Optional[dict[str, Any]] = None,
        network_timeout: int = 30,
        fetch: bool = False,
    ) -> Any:
        """
        Centralized query execution method.

        :param template_name: Name of the SQL template file
        :param params: Parameters to format into the SQL query
        :param network_timeout: Connection timeout
        :param fetch: Whether to fetch results
        :param cursor_type: Type of cursor to use ('default', 'dict')
        :return: Query results or None
        """
        with self.connection(network_timeout=network_timeout) as conn:
            template_path = os.path.join(PATH, "templates", template_name)
            sql = open(template_path).read()

            if params is None:
                params = {}
            if "name" not in params:
                params["name"] = self.name

            cursor = conn.cursor(connector.cursor.DictCursor)

            try:
                cursor.execute(sql.format(**params), params)
                if fetch:
                    return cursor.fetchall()
                conn.commit()
                return None

            except Exception as e:
                # TODO: Implement proper logging
                print(f"Error executing query {template_name}: {e}")
                conn.rollback()
                raise

    def connection(self, network_timeout: int):
        return connector.connect(network_timeout=network_timeout, **self.conn_params)

    def initialise_mq(self, name: str = "message_queue", fresh: bool = False):
        """Create Message Queue and Dead Letter Queue Tables."""
        sql = open(os.path.join(PATH, "templates", "initialise_db.sql")).read()
        exists = ""
        if fresh is False:
            exists = "if not exists"

        sql = sql.format(exists=exists, name=name, db_json_type="variant")
        self._execute_query(sql)

    def publish_messages(self, messages: list[Message]) -> list[MessageId]:
        """Publish messages to the Snowflake message queue."""
        published_ids = []

        for message in messages:
            try:
                self._execute_query(
                    "publish_messages.sql",
                    params={
                        "id": str(message.id),
                        "message_type": message.message_type.name,
                        "data": json.dumps(message.data),
                        "priority": message.priority.name,
                        "delay": message.delay,
                        "max_attempts": message.max_attempts,
                    },
                )
                published_ids.append(message.id)
            except Exception as e:
                # TODO: Implement proper error logging
                print(f"Error publishing message {message.id}: {e}")
                # Consider adding to DLQ or implementing retry logic

        return published_ids

    def consume_messages(self, n: int = 1) -> list[Message]:
        """Consume messages from the Snowflake message queue."""
        consumed_messages = []

        try:
            rows = self._execute_query(
                "consume_messages.sql",
                params={"limit": n},
                fetch=True,
            )

            for row in rows:
                message = Message(
                    id=uuid.UUID(row["ID"]),
                    message_type=MessageType[row["MESSAGE_TYPE"]],
                    data=json.loads(row["DATA"]),
                    priority=Priority[row["PRIORITY"]],
                    delay=row["DELAY"],
                    attempts=row["ATTEMPTS"],
                    max_attempts=row["MAX_ATTEMPTS"],
                )
                consumed_messages.append(message)
        except Exception as e:
            # TODO: Implement proper error logging
            print(f"Error consuming messages: {e}")

        return consumed_messages

    def consume_messages_by_id(self, ids: list[MessageId]) -> list[Message]:
        return [DEFAULT_MESSAGE for _ in ids]

    def retry_messages(self, ids: list[MessageId]) -> list[MessageId]:
        return [DEFAULT_MESSAGE_ID for _ in ids]

    def message_statuses(self, ids: list[MessageId]) -> list[tuple[MessageId, Status]]:
        return [(DEFAULT_MESSAGE_ID, Status.COMPLETED) for _ in ids]

    def fetch_dlq(self, n: int) -> list[Message]:
        return [DEFAULT_MESSAGE for _ in range(n)]

    def clean_mq(self) -> None:
        return None


class Mq(MessageQueue):
    def __init__(self, db: Db):
        self.db = db

    def publish(self, messages: list[Message]) -> list[MessageId]:
        return self.db.publish_messages(messages)

    def consume(self, n: int = 1) -> list[Message]:
        return self.db.consume_messages(n)

    def consume_by_id(self, ids: list[MessageId]) -> list[Message]:
        return self.db.consume_messages_by_id(ids)

    def retry(self, ids: list[MessageId]) -> list[MessageId]:
        return self.db.retry_messages(ids)

    def statuses(self, ids: list[MessageId]) -> list[tuple[MessageId, Status]]:
        return self.db.message_statuses(ids)

    def dlq(self, n: int = 10) -> list[Message]:
        return self.db.fetch_dlq(n)

    def clean(self) -> None:
        return self.db.clean_mq()
