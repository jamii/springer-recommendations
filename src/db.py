"""Use leveldb as a simple key->set-of-values store"""

import leveldb
import cPickle as pickle
import os
import os.path

import disco.core

import settings

def insert(input, db_name, collection_name):
    db = DB(db_name, collection_name)
    for key, values in disco.core.result_iterator(input):
        db.insert(key, values)
    db.sync()

class DB():
    def __init__(self, db_name, collection_name):
        dir_name = os.path.join(settings.root_directory, db_name, collection_name)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        self.db = leveldb.LevelDB(dir_name)

    def get(self, key):
        values = pickle.loads(self.db.Get(key))
        assert (type(values) is set)
        return values

    def get_multi(self, keys):
        return dict(((key, self.get(key)) for key in keys))

    def insert(self, key, new_values):
        try:
            old_values = self.get(key)
        except KeyError:
            old_values = set()
        values = old_values.union(new_values)
        self.db.Put(key, pickle.dumps(values))

    def sync(self):
        # complete hack :(
        key, values = self.db.RangeIter().next()
        self.db.Put(key, values, sync=True)

    def __iter__(self):
        for key, values in self.db.RangeIter():
            yield key, pickle.loads(values)
