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

id_struct = struct.Struct('L')

class String2Id(Abstract):
    def __init__(self, build_name, db_name, batch_size=1000):
        self.batch_dict = {}
        Abstract.__init__(self, build_name, db_name, batch_size)

    def get(self, string):
        try:
            return self.batch_dict[string]
        except KeyError:
            id, = id_struct.unpack(self.db.Get(string))
            return id

    def put(self, string, id):
        self.batch_dict[string] = id
        Abstract.put(self, string, id_struct.pack(id))

    def sync(self, really=True):
        self.batch_dict.clear()
        Abstract.sync(self, really)

    def __iter__(self):
        self.sync()
        for string, id in self.db.RangeIter():
            yield string, id_struct.unpack(id)[0]

class Id2String(Abstract):
    def get(self, id):
        self.sync()
        return self.db.Get(id_struct.pack(id))

    def put(self, id, string):
        Abstract.put(self, id_struct.pack(id), string)

    def __iter__(self):
        self.sync()
        for id, string in self.db.RangeIter():
            yield id_struct.unpack(id)[0], string

class Ids():
    """A bijection between string keys and auto-assigned integer ids"""

    def __init__(self, build_name, string_name, batch_size=1000):
        self.string2id = String2Id(build_name, "%s2id" % string_name, batch_size)
        self.id2string = Id2String(build_name, "id2%s" % string_name, batch_size)
        try:
            self.next_id = max((id for id, _ in self.id2string)) + 1
        except ValueError:
            self.next_id = 0

    def get_id(self, string):
        try:
            return self.string2id.get(string)
        except KeyError:
            id = self.next_id
            self.next_id += 1
            self.string2id.put(string, id)
            self.id2string.put(id, string)
            return id

    def get_string(self, id):
        return self.id2string.get(id)
