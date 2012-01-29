import collections
import heapq

import db

def db_value_lengths(db_name, collection_name):
    counter = collections.Counter()

    for key, values in db.DB(db_name, collection_name):
        counter[len(values)] += 1

    return [counter[i] for i in xrange(0, max(counter.keys()) + 1)]

def db_value_extremes(db_name, collection_name, n):
    extremes = []

    def lengths():
        for key, values in db.DB(db_name, collection_name):
            yield len(values), key

    return heapq.nlargest(n, lengths())
