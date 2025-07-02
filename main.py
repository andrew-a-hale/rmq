import uuid

import tomllib

from src import mq, snowflake_mq


def main():
    with open(".snowflake/connections.toml", mode="rb") as toml:
        conn_params = tomllib.load(toml).get("default")
        assert isinstance(conn_params, dict)

    sf = snowflake_mq.Db(name="message_queue", conn_params=conn_params, fresh=True)
    queue = snowflake_mq.Mq(sf)

    message = mq.Message(
        uuid.uuid4(),
        mq.MessageType.ModelOne,
        {"data": "!"},
        mq.Priority.IMMEDIATE,
    )
    queue.publish([message])
    queue.statuses([message.id])
    messages = queue.consume()
    queue.execute(messages[0], lambda x: x)
    queue.statuses([message.id])
    queue.clean()


if __name__ == "__main__":
    main()
