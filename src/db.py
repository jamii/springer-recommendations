"""Use mongodb as a simple key->set-of-values store"""

import collections
import pymongo

import settings
import mr
import cache

class DBInsert(mr.Job):
    @staticmethod
    def map_init(iter, params):
        params['collection'] = DB(params['db_name'], params['collection_name'])

    @staticmethod
    @mr.map_with_errors
    def map((key, values), params):
        params['collection'].update(key, values)
        return () # have to return an iterable :(

def insert(input, db_name, collection_name):
    job = DBInsert().run(input=input, params={'db_name':db_name, 'collection_name':collection_name})
    job.wait()
    job.purge()

def drop(db_name):
    pymongo.Connection().drop_database(db_name)

class DB():
    def __init__(self, db_name, collection_name):
        self.collection = pymongo.Connection()[db_name][collection_name]
        self.collection.ensure_index([('k',1),('v',1)], drop_dups=True, unique=True)
        self.cache = cache.Random(max_size=settings.db_cache_size)

    def update(self, key, values):
        for value in values:
            self.collection.insert({'k':key, 'v':value})

    def get(self, key):
        if key in self.cache:
            return self.cache[key]
        else:
            values = [item['v'] for item in self.collection.find({'k':key})]
            self.cache[key] = values
            return values

    def get_multi(self, keys):
        cached = dict(((key, self.cache[key]) for key in keys if key in self.cache))

        uncached_keys = [key for key in keys if key not in self.cache]
        uncached = collections.defaultdict(list)
        for item in self.collection.find({'k':{'$in':uncached_keys}}):
            uncached[item['k']].append(item['v'])

        for key, values in uncached.items():
            self.cache[key] = values

        cached.update(uncached)
        return cached

    def __iter__(self):
        for item in self.collection.find():
            yield item['k'], item['v']
