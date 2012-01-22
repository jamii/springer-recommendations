"""
Simple cache using random eviction
"""

import random

class Random():
    def __init__(self, max_size):
        self.max_size = max_size
        self.cache = {}
        self.keys = [None]*max_size

    def __contains__(self, key):
        return key in self.cache

    def __getitem__(self, key):
        return self.cache[key]

    def __setitem__(self, key, value):
        if key in self.cache:
            self.cache[key] = value
        else:
            index = random.randint(0, self.max_size-1)

            old_key = self.keys[index]
            if old_key:
                del self.cache[old_key]

            self.keys[index] = key
            self.cache[key] = value
