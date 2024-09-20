import threading 

class LRUCache_Class:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = {}
        self.usage_order = []
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            if key in self.cache:
                # Move the accessed key to the end to represent it as most recently used
                self.usage_order.remove(key)
                self.usage_order.append(key)
                return self.cache[key]
            else:
                return -1  # Return -1 if key not found

    def put(self, key, value):
        with self.lock:
            if key in self.cache:
                # Update the value and move the key to the end to represent it as most recently used
                self.cache[key] = value
                self.usage_order.remove(key)
                self.usage_order.append(key)
            else:
                if len(self.cache) >= self.capacity:
                    # Remove the least recently used key
                    lru_key = self.usage_order.pop(0)
                    del self.cache[lru_key]
                # Add the new key-value pair to the cache and usage order
                self.cache[key] = value
                self.usage_order.append(key)

    def remove(self,key):
        with self.lock:
            if key in self.cache:
                # Move the accessed key to the end to represent it as most recently used
                self.usage_order.remove(key)
                del self.cache[key]


