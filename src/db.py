"""Use mongodb as a simple key-value store, since its already present for logs"""

import pymongo

import mr

def open(db_name, collection_name):
    collection = pymongo.Connection()[db_name][collection_name]
    return collection

class DBInsert(mr.Job):
    @staticmethod
    def map_init(iter, params):
        params['collection'] = open(params['db_name'], params['collection_name'])

    @staticmethod
    @mr.map_with_errors
    def map((key, value), params):
        params['collection'].save({'_id':key, 'value':value})
        return () # have to return an iterable :(

def insert(input, db_name, collection_name):
    job = DBInsert().run(input=input, params={'db_name':db_name, 'collection_name':collection_name})
    job.wait()
    job.purge()

def get(collection, key):
    return collection.find_one({'_id':key})['value']
