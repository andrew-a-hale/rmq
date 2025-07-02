import json
import logging
import os
import uuid
from typing import Any, Callable, Optional

from snowflake import connector
from snowflake.connector.result_set import ResultSet

from src.db import DatabaseConnector
from src.mq import Message, MessageId, MessageQueue, MessageType, Priority, Status

PATH = os.path.dirname(__file__)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class Db(DatabaseConnector):
    def __init__(self, name: str, conn_params: dict[str, str], fresh: bool = False):
        self.name = name
        self.dlq = "dlq_" + name
        self.db_json_type = "variant"
        self.conn_params = conn_params
        logger.info(f"Initializing Snowflake message queue: {name}")
        self.initialise_mq(fresh)

    def _execute_query(
        self,
        template_name: str,
        params: Optional[dict[str, Any]] = None,
        network_timeout: int = 30,
        fetch: int = -1,
    ) -> ResultSet | None:
        """
        Centralized query execution method.

        :param template_name: Name of the SQL template file
        :param params: Parameters to format into the SQL query
        :param network_timeout: Connection timeout
        :param fetch: Return nth (0-indexed) result, -1 returns none
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
            if "dlq" not in params:
                params["dlq"] = self.dlq
            if "db_json_type" not in params:
                params["db_json_type"] = self.db_json_type

            cursor = conn.cursor(connector.cursor.DictCursor)
            results = []

            sql = sql.format(**params)
            stmts = [stmt for stmt in sql.split(";") if stmt.strip()]
            for stmt in stmts:
                try:
                    cursor.execute(stmt, params)
                    results.append(cursor.fetchall())

                except Exception as e:
                    params_json = json.dumps(params)
                    logger.error(
                        f"Error executing query {template_name} with params: {params_json}: {e}",
                        exc_info=True,
                    )
                    conn.rollback()
                    raise e

            conn.commit()

            if 0 <= fetch < len(results):
                return results[fetch]

    def connection(self, network_timeout: int):
        return connector.connect(network_timeout=network_timeout, **self.conn_params)

    def initialise_mq(self, fresh: bool = False):
        """Create Message Queue and Dead Letter Queue Tables."""
        exists = ""
        if fresh:
            self._execute_query("nuke.sql")
            self._execute_query(
                "initialise_db.sql",
                params={"exists": exists},
            )
        else:
            exists = "if not exists"
            self.clean_mq()

    def publish_messages(self, messages: list[Message]) -> list[MessageId]:
        """Publish messages to the Snowflake message queue."""
        published_ids = []
        logger.info(f"Publishing {len(messages)} messages to queue")

        for message in messages:
            try:
                self._execute_query(
                    "publish_messages.sql",
                    params={
                        "id": str(message.id),
                        "message_type": message.message_type.name,
                        "payload": json.dumps(message.payload),
                        "priority": message.priority.name,
                        "delay": message.delay,
                        "max_attempts": message.max_attempts,
                        "new": Status.NEW.value,
                    },
                )
                published_ids.append(message.id)
            except Exception as e:
                logger.error(
                    f"Error publishing message {message.id}: {e}",
                    exc_info=True,
                )

        return published_ids

    def consume_messages(self, n: int = 1) -> list[Message]:
        """Consume messages from the Snowflake message queue."""
        consumed_messages = []

        try:
            rows = self._execute_query(
                "consume_messages.sql",
                params={
                    "limit": n,
                    "new": Status.NEW.value,
                    "processing": Status.PROCESSING.value,
                    "failed": Status.FAILED.value,
                },
                fetch=1,
            )

            if rows is None:
                return consumed_messages

            for row in rows:
                message = Message(
                    id=uuid.UUID(row["ID"]),
                    message_type=MessageType[row["MESSAGE_TYPE"]],
                    payload=json.loads(row["PAYLOAD"]),
                    priority=Priority[row["PRIORITY"]],
                    delay=row["DELAY"],
                    attempts=row["ATTEMPTS"],
                    max_attempts=row["MAX_ATTEMPTS"],
                )
                consumed_messages.append(message)
        except Exception as e:
            logger.error(f"Error consuming messages row: {row}: {e}", exc_info=True)

        return consumed_messages

    def consume_messages_by_id(self, ids: list[MessageId]) -> list[Message]:
        """Consume messages from the Snowflake message queue."""
        consumed_messages = []

        try:
            rows = self._execute_query(
                "consume_messages_by_id.sql",
                params={
                    "ids": [str(id) for id in ids],
                    "new": Status.NEW.value,
                    "processing": Status.PROCESSING.value,
                    "failed": Status.FAILED.value,
                },
                fetch=1,
            )

            if rows is None:
                return consumed_messages

            for row in rows:
                message = Message(
                    id=uuid.UUID(row["ID"]),
                    message_type=MessageType[row["MESSAGE_TYPE"]],
                    payload=json.loads(row["PAYLOAD"]),
                    priority=Priority[row["PRIORITY"]],
                    delay=row["DELAY"],
                    attempts=row["ATTEMPTS"],
                    max_attempts=row["MAX_ATTEMPTS"],
                )
                consumed_messages.append(message)
        except Exception as e:
            logger.error(f"Error consuming messages: {e}", exc_info=True)

        return consumed_messages

    def retry_messages(self, n: int = 1) -> list[Message]:
        """Retry messages from the Snowflake message queue."""
        retry_messages = []

        try:
            rows = self._execute_query(
                "retry_messages.sql",
                params={
                    "n": n,
                    "failed": Status.FAILED.value,
                    "processing": Status.PROCESSING.value,
                },
                fetch=1,
            )

            if rows is None:
                return retry_messages

            for row in rows:
                message = Message(
                    id=uuid.UUID(row["ID"]),
                    message_type=MessageType[row["MESSAGE_TYPE"]],
                    payload=json.loads(row["PAYLOAD"]),
                    priority=Priority[row["PRIORITY"]],
                    delay=row["DELAY"],
                    attempts=row["ATTEMPTS"],
                    max_attempts=row["MAX_ATTEMPTS"],
                )
                retry_messages.append(message)
        except Exception as e:
            logger.error(f"Error retrying messages: {e}", exc_info=True)

        return retry_messages

    def retry_messages_by_id(self, ids: list[MessageId]) -> list[Message]:
        """Retry messages from the Snowflake message queue."""
        retry_messages = []

        try:
            rows = self._execute_query(
                "retry_messages.sql",
                params={
                    "ids": [str(id) for id in ids],
                    "failed": Status.FAILED.value,
                    "processing": Status.PROCESSING.value,
                },
                fetch=1,
            )

            if rows is None:
                return retry_messages

            for row in rows:
                message = Message(
                    id=uuid.UUID(row["ID"]),
                    message_type=MessageType[row["MESSAGE_TYPE"]],
                    payload=json.loads(row["PAYLOAD"]),
                    priority=Priority[row["PRIORITY"]],
                    delay=row["DELAY"],
                    attempts=row["ATTEMPTS"],
                    max_attempts=row["MAX_ATTEMPTS"],
                )
                retry_messages.append(message)
        except Exception as e:
            logger.error(f"Error retrying messages: {e}", exc_info=True)

        return retry_messages

    def retry_dlq_messages(self, n: int) -> list[Message]:
        """Retry messages from the Snowflake message queue."""
        retry_messages = []

        try:
            rows = self._execute_query(
                "retry_dlq_messages.sql",
                params={
                    "n": n,
                    "failed": Status.FAILED.value,
                    "processing": Status.PROCESSING.value,
                },
                fetch=2,
            )

            if rows is None:
                return retry_messages

            for row in rows:
                message = Message(
                    id=uuid.UUID(row["ID"]),
                    message_type=MessageType[row["MESSAGE_TYPE"]],
                    payload=json.loads(row["PAYLOAD"]),
                    priority=Priority[row["PRIORITY"]],
                    delay=row["DELAY"],
                    attempts=row["ATTEMPTS"],
                    max_attempts=row["MAX_ATTEMPTS"],
                )
                retry_messages.append(message)
        except Exception as e:
            logger.error(
                f"Error retrying dlq messages with row: {row}: {e}", exc_info=True
            )

        return retry_messages

    def message_statuses(self, ids: list[MessageId]) -> list[tuple[MessageId, Status]]:
        """Message statuses from the Snowflake message queue."""
        try:
            rows = self._execute_query(
                "message_statuses.sql",
                params={"ids": [str(id) for id in ids]},
                fetch=0,
            )

            if rows is None:
                return []

            statuses = rows

        except Exception as e:
            logger.error(f"Error getting messages statuses: {e}", exc_info=True)
            statuses = []

        return statuses

    def fetch_dlq(self, n: int) -> list[Message]:
        """Message statuses from the Snowflake message queue."""
        messages = []

        try:
            rows = self._execute_query(
                "fetch_dlq.sql",
                params={"n": n},
                fetch=0,
            )

            if rows is None:
                return messages

            for row in rows:
                message = Message(
                    id=uuid.UUID(row["ID"]),
                    message_type=MessageType[row["MESSAGE_TYPE"]],
                    payload=json.loads(row["PAYLOAD"]),
                    priority=Priority[row["PRIORITY"]],
                    delay=row["DELAY"],
                    attempts=row["ATTEMPTS"],
                    max_attempts=row["MAX_ATTEMPTS"],
                )
                messages.append(message)

        except Exception as e:
            logger.error(f"Error getting dlq: {e}", exc_info=True)

        return messages

    def clean_mq(self) -> None:
        try:
            self._execute_query(
                "clean_mq.sql",
                params={
                    "completed": Status.COMPLETED.value,
                    "processing": Status.PROCESSING.value,
                    "failed": Status.FAILED.value,
                },
            )
        except Exception as e:
            logger.error(f"Error cleaning the message queue: {e}", exc_info=True)

    def complete_message(self, id: MessageId) -> None:
        """Mark a message as completed from the Snowflake message queue."""
        try:
            self._execute_query(
                "complete_message.sql",
                params={
                    "id": str(id),
                    "completed": Status.COMPLETED.value,
                },
            )
        except Exception as e:
            logger.error(f"Error completing messages: {e}", exc_info=True)

    def fail_message(self, id: MessageId) -> None:
        """Mark a message as completed from the Snowflake message queue."""
        try:
            self._execute_query(
                "fail_message.sql",
                params={
                    "id": str(id),
                    "failed": Status.FAILED.value,
                },
            )
        except Exception as e:
            logger.error(f"Error failing messages: {e}", exc_info=True)


class Mq(MessageQueue):
    def __init__(self, db: Db):
        self.db = db

    def publish(self, messages: list[Message]) -> list[MessageId]:
        return self.db.publish_messages(messages)

    def consume(self, n: int = 1) -> list[Message]:
        return self.db.consume_messages(n)

    def consume_by_id(self, ids: list[MessageId]) -> list[Message]:
        if len(ids) == 0:
            return []
        return self.db.consume_messages_by_id(ids)

    def retry(self, n: int = 1) -> list[Message]:
        return self.db.retry_messages(n)

    def retry_by_id(self, ids: list[MessageId]) -> list[Message]:
        if len(ids) == 0:
            return []
        return self.db.retry_messages_by_id(ids)

    def retry_dlq(self, n: int = 1) -> list[Message]:
        return self.db.retry_dlq_messages(n)

    def statuses(self, ids: list[MessageId]) -> list[tuple[MessageId, Status]]:
        if len(ids) == 0:
            return []
        return self.db.message_statuses(ids)

    def dlq(self, n: int = 10) -> list[Message]:
        self.db.clean_mq()
        return self.db.fetch_dlq(n)

    def clean(self) -> None:
        self.db.clean_mq()

    def complete(self, id: MessageId) -> None:
        self.db.complete_message(id)

    def fail(self, id: MessageId) -> None:
        self.db.fail_message(id)

    def execute(self, message: Message, handler: Callable):
        try:
            handler(message)
        except Exception as e:
            logger.error(
                f"Error: failed to call handler function on message: {message} with {e}",
                exc_info=True,
            )
            self.fail(message.id)
        else:
            self.complete(message.id)
