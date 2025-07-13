import asyncio
import uuid

import tomllib

from src import mq
from src.sf import mq as sfmq


async def main():
    with open(".snowflake/connections.toml", mode="rb") as toml:
        conn_params = tomllib.load(toml).get("mq")
        assert isinstance(conn_params, dict)

    sf = sfmq.Db(name="message_queue", conn_params=conn_params, fresh=True)
    queue = sfmq.Mq(sf)

    message = mq.Message(
        uuid.uuid4(),
        mq.MessageType.ModelOne,
        {"data": "!"},
        mq.Priority.IMMEDIATE,
    )
    another = mq.Message(
        uuid.uuid4(),
        mq.MessageType.ModelTwo,
        {"data": "?"},
        mq.Priority.IMMEDIATE,
    )
    queue.publish([message, another])
    queue.statuses([message.id])

    messages = queue.consume(2)

    async def handler(x: mq.Message):
        print("executing!")
        print("Message: ", x.deserialise(True))
        return x

    jobs = []
    for message in messages:
        print("started: ", message.id)
        jobs.append(queue.execute(message, handler))

    print(queue.statuses([message.id for message in messages]))
    await asyncio.gather(*jobs)
    print(queue.statuses([message.id for message in messages]))

    queue.clean()


if __name__ == "__main__":
    asyncio.run(main())
