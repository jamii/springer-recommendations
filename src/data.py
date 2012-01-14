import datetime
import math

import util

class Histogram():
    def __init__(self, items, min_key, max_key):
        # min_key and max_key are inclusive
        self.min_key = min_key
        self.max_key = max_key
        self.counts = {}
        for item in items:
            self.counts[item] = self.counts.get(item, 0) + 1

    def __str__(self):
        return str(dict([(k, self[k]) for k in self]))

    def __repr__(self):
        return repr(dict([(k, self[k]) for k in self]))

    def __contains__(self, item):
        if item in self.counts:
            return True
        elif self.min_key <= item <= self.max_key:
            return True
        else:
            return False

    def __getitem__(self, item):
        if item in self.counts:
            return self.counts[item]
        elif self.min_key <= item <= self.max_key:
            return 0
        else:
            raise KeyError(item)

    def __iter__(self):
        if type(self.min_key) is int:
            return iter(xrange(self.min_key, self.max_key+1))
        elif type(self.min_key) is datetime.date:
            return util.date_range(self.min_key, self.max_key)

    def group_by(self, fun):
        # require that fun is monotonic
        self.min_key = fun(self.min_key)
        self.max_key = fun(self.max_key)
        counts = {}
        for key, count in self.counts.items():
            counts[fun(key)] = counts.get(key, 0) + count
        self.counts = counts

    def total(self):
        return sum(self.counts.values())
