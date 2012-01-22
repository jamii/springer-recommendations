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

    def update(self, key, values):
        self.collection.update({'_id':key}, {'$addToSet':{'values':{'$each':values}}}, upsert=True)

    @cache.lfu(maxsize=500)
    def get(self, key):
        return self.collection.find_one({'_id':key})['value']

    def get_multi(self, keys):
        cached = dict(((key, self.get.cache[key]) for key in keys if key in self.get.cache))

        uncached_keys = [key for key in keys if key not in self.get.cache]
        db_results = self.collection.find({'_id':{'$in':uncached_keys}})
        uncached = dict(((item['_id'], item['value']) for item in db_results))

        for key, value in uncached.items():
            self.get.push(key, value)

        cached.update(uncached)
        return cached
