from redis import Redis


def load(client: Redis, key: str, value):
    # TODO log before write
    client.set(key, value)
