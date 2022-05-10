import redis

class RedisWork():
    def __init__(self) -> None:
        self.r = redis.Redis(host="localhost", port=6379, db=0)

    def set_key_value(self, key, value):
        self.r.set(key, value)

    def check_key(self, key):
        val = self.r.get(key)
        if val:
            return True
        else: 
            return False

    def value_of_key(self, key):
        if self.r.get(key):
            val = self.r.get(key)
        return val
