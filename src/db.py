"""Use mongodb as a simple key->set-of-values store"""

import pymongo

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

class DB():
    def __init__(self, db_name, collection_name):
        self.collection = pymongo.Connection()[db_name][collection_name]
        self.cache = cache.Random(max_size=settings.db_cache_size)

    def update(self, key, values):
        self.collection.update({'_id':key}, {'$addToSet':{'values':{'$each':values}}}, upsert=True)

    def get(self, key):
        if key in self.cache:
            return self.cache[key]
        else:
            values = self.collection.find_one({'_id':key})['values']
            self.cache[key] = values
            return values

    def get_multi(self, keys):
        cached = dict(((key, self.cache[key]) for key in keys if key in self.cache))

        uncached_keys = [key for key in keys if key not in self.cache]
        db_results = self.collection.find({'_id':{'$in':uncached_keys}})
        uncached = dict(((item['_id'], item['values']) for item in db_results))

        for key, values in uncached.items():
            self.cache[key] = values

        cached.update(uncached)
        return cached
