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
    def __init__(self, build_name, db_name, batch_size=1000):
        dir_name = os.path.join(settings.root_directory, build_name, db_name)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        self.db = leveldb.LevelDB(dir_name)
        self.batch_size = batch_size
        self.__puts = 0
        self.__batch = leveldb.WriteBatch()

    def put(self, key, value):
        self.__batch.Put(key, value)
        self.__puts += 1
        if self.__puts == self.batch_size:
            self.db.Write(self.__batch)
            self.__puts = 0
            self.__batch = leveldb.WriteBatch()

    def sync(self, really=True):
        if self.__puts > 0:
            self.db.Write(self.__batch, sync=really)
            self.__puts = 0
            self.__batch = leveldb.WriteBatch()

    def __del__(self):
        self.sync()

class SingleValue(Abstract):
    """Maps each string key to a single pickled value"""
    def get(self, key):
        self.sync()
        return pickle.loads(self.db.Get(key))

    def put(self, key, value):
        Abstract.put(self, key, pickle.dumps(value))

    def __iter__(self):
        self.sync()
        for key, values in self.db.RangeIter():
            yield key, pickle.loads(values)

key_struct = struct.Struct('64p')
value_struct = struct.Struct('64x 64p')
pair_struct = struct.Struct('64p 64p')

def pack_key(key):
    return key_struct.pack(key)

def pair(key, value):
    return pair_struct.pack(key, value)

def unpair(string):
    return pair_struct.unpack(string)

def unpack_value(string):
    value, = value_struct.unpack(string)
    return value

class MultiValue(Abstract):
    """Maps each string key to a set of strings which can be updated incrementally"""

    def __iterget(self, key):
        for kv in self.db.RangeIter(key_from=pack_key(key)+chr(0), key_to=pack_key(key)+chr(255), include_value=False):
            yield unpack_value(kv)

    def get(self, key):
        self.sync()
        return list(self.__iterget(key))

    def put(self, key, value):
        Abstract.put(self, pair(key, value), "")

    def __kv_iter(self):
        for kv in self.db.RangeIter(include_value=False):
            yield unpair(kv)

    def __iter__(self):
        self.sync()
        for key, values in itertools.groupby(self.__kv_iter(), lambda (key, value): key):
            yield key, [value for (key,value) in values]
