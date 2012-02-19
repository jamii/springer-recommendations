"""Lightweight wrappers around leveldb"""

import leveldb
import cPickle as pickle
import struct
import os
import os.path
import itertools

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

pair_struct = struct.Struct('I')

def pack(string):
    return pair_struct.pack(len(string)) + string

def pair(key, value):
    return pack(key) + pack(value)

def unpair(string):
    key_len, = pair_struct.unpack(string[0:4])
    value_len, = pair_struct.unpack(string[4:8])
    return string[2:2+key_len], string[2+key_len:2+key_len+value_len]

def value(string):
    key_len = pair_struct.unpack(string[0:4])
    value_len = pair_struct.unpack(string[4:8])
    return string[2+key_len:2+key_len+value_len]

class MultiValue(Abstract):
    """Maps each string key to a set of strings which can be updated incrementally"""

    def iterget(self, key):
        assert (self.mode == 'r')
        for kv in self.db.RangeIter(key_from=pack(key)+chr(0), key_to=pack(key)+chr(255), include_value=False):
            yield value(kv)

    def get(self, key):
        return list(self.iterget(key))

    def put(self, key, value):
        assert (self.mode == 'w')
        Abstract.put(self, pair(key, value), "")

    def __kv_iter(self):
        for kv in self.db.RangeIter(include_value=False):
            yield unpair(kv)

    def __iter__(self):
        assert (self.mode == 'r')
        for key, values in itertools.groupby(self.__kv_iter(), lambda (key, value): key):
            yield key, (value for (key,value) in values)
