"""Lightweight wrappers around leveldb"""

import leveldb
import cPickle as pickle
import os
import os.path

import disco.core

import settings

class Abstract():
    def __init__(self, build_name, db_name, mode, batch_size=1000):
        assert (mode in 'rw')
        self.mode = mode
        dir_name = os.path.join(settings.root_directory, build_name, db_name)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        self.db = leveldb.LevelDB(dir_name)
        self.batch_size = batch_size
        self.__puts = 0
        self.__batch = leveldb.WriteBatch()

    def put(self, key, value):
        assert (self.mode == 'w')
        self.__batch.Put(key, value)
        self.__puts += 1
        if self.__puts == self.batch_size:
            self.db.Write(self.__batch)
            self.__puts = 0
            self.__batch = leveldb.WriteBatch()

    def sync(self, really=True):
        self.db.Write(self.__batch, sync=True)
        self.__puts = 0
        self.__batch = leveldb.WriteBatch()

class SingleValue(Abstract):
    """Maps each string key to a single pickled value"""
    def get(self, key):
        assert (self.mode == 'r')
        return pickle.loads(self.db.Get(key))

    def put(self, key, value):
        assert (self.mode == 'w')
        Abstract.put(self, key, pickle.dumps(value))

    def __iter__(self):
        assert (self.mode == 'r')
        for key, values in self.db.RangeIter():
            yield key, pickle.loads(values)

SEP = chr(0)
END = chr(1)

def no_seps(string):
    try:
        string.index(SEP)
        return False
    except ValueError:
        try:
            string.index(END)
            return False
        except ValueError:
            return True

class MultiValue(Abstract):
    """Maps each string key to a set of strings which can be updated incrementally"""

    def iterget(self, key):
        assert (self.mode == 'r')
        assert (no_seps(key))
        for kv in self.db.RangeIter(key_from=key+SEP, key_to=key+END, include_value=False):
            i = kv.index(SEP)
            yield kv[i+1:]

    def get(self, key):
        return list(self.iterget(key))

    def put(self, key, value):
        assert (self.mode == 'w')
        assert (no_seps(key))
        assert (no_seps(value))
        Abstract.put(self, key + SEP + value, "")

    def __kv_iter(self):
        for kv in self.db.RangeIter(include_value=False):
            i = kv.index(SEP)
            yield kv[:i], kv[i+1:]

    def __iter__(self):
        assert (self.mode == 'r')
        current_key = None
        current_values = []
        for key, value in self.__kv_iter():
            if key == current_key:
                current_values.append(value)
            else:
                if current_key is not None:
                    yield current_key, current_values
                current_key = key
                current_values = [value]
        if current_key is not None:
            yield current_key, current_values
