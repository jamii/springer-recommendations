"""Parse download logs dumped from mongodb"""

import bson
import struct

# for some reason the dates are stored as integers...
def date_to_int(date):
    return date.year * 10000 + date.month * 100 + date.day
def int_to_date(int):
    return datetime.date(int // 10000, int % 10000 // 100, int % 100)

unpack_prefix = struct.Struct('i').unpack

def from_dump(filename):
    file = open(filename, 'rb')
    while True:
        prefix = file.read(4)
        if len(prefix) == 0:
            break
        elif len(prefix) != 4:
            raise IOError("Prefix is too short: %s" % prefix)
        else:
            size, = unpack_prefix(prefix)
            data = prefix + file.read(size - 4)
            yield bson.BSON(data).decode()
