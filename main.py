import tomllib
from src import snowflake_mq


def main():
    with open(".snowflake/connections.toml", mode="rb") as toml:
        conn_params = tomllib.load(toml).get("default")
        assert isinstance(conn_params, dict)

    db = snowflake_mq.Db(name="message_queue", conn_params=conn_params)
    mq = snowflake_mq.Mq(db)
    mq.dlq()


if __name__ == "__main__":
    main()
