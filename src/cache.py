"""
Simple cache using random eviction
"""

import random

class Random():
    def __init__(self, max_size):
        self.max_size = max_size
        self.cache = {}
        self.keys = [None]*max_size
        self.total = 0

    def __contains__(self, key):
        return key in self.cache

    def __getitem__(self, key):
        return self.cache[key]

    def evict(self):
        index = random.randint(0, self.max_size-1)
        old_key = self.keys[index]
        del self.cache[old_key]
        return index

    def __setitem__(self, key, value):
        if key in self.cache:
            self.cache[key] = value
        else:
            if self.total >= self.max_size:
                index = self.evict()
            else:
                index = self.total
                self.total += 1

            self.keys[index] = key
            self.cache[key] = value
