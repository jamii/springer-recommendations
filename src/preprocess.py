"""Preprocess download logs dumped from mongodb"""

import os
import struct
import subprocess
import tempfile

import bson

import util

# For some reason the dates are stored as integers...
def date_to_int(date):
    return date.year * 10000 + date.month * 100 + date.day
def int_to_date(int):
    return datetime.date(int // 10000, int % 10000 // 100, int % 100)

unpack_prefix = struct.Struct('i').unpack

# TODO: might be worth manually decoding the bson here and just picking out si/doi. could also avoid the utf8 encode later.
def _from_dump(dump_filename):
    """Read a mongodb dump containing bson-encoded download logs"""
    dump_file = open(dump_filename, 'rb')

    while True:
        prefix = dump_file.read(4)
        if len(prefix) == 0:
            break
        elif len(prefix) != 4:
            raise IOError('Prefix is too short: %s' % prefix)
        else:
            size, = unpack_prefix(prefix)
            data = prefix + dump_file.read(size - 4)
            download = bson.BSON(data).decode()
            if download.get('si', '') and download.get('doi', ''):
                yield download

def from_dump(dump_filename):
    return util.logged('from_dump', _from_dump(dump_filename))

# We're going to build up abstractions allowing us to treat temporary files somewhat like python lists

def temp_file():
    # We make the tempfile in the current directory because the target machine has little space on /tmp
    file = tempfile.NamedTemporaryFile(dir="/mnt/var/springer-recommendations/")
    util.log('temp_file', file.name)
    return file

def count_lines(in_file):
    """Returns len(in_file)"""
    util.log('count_lines', 'starting')
    result = subprocess.check_output(['wc', '-l', in_file.name])
    count, _ = result.split()
    util.log('count_lines', 'finished')
    return int(count)

def uniq_sorted_file(in_file):
    """Returns uniq(sort(in_file))"""
    util.log('uniq_sorted_file', 'starting')
    out_file = temp_file()
    subprocess.check_call(['sort', '-u', in_file.name, '-o', out_file.name])
    util.log('uniq_sorted_file', 'finished')
    return out_file

# TODO: assumes there are no zero bytes in fst or snd :(
def pack_pair(fst, snd):
    return '%s\x00%s' % (fst, snd)

def unpack_pair(pair):
    return pair.split('\x00')

def labelled_file(in_file, label_file):
    """Returns [(label_file.index(snd), fst) for fst,snd in in_file]. Requires both files to be sorted."""
    util.log('labelled_file', 'starting')
    out_file = temp_file()
    label = label_file.readline().rstrip()
    index = 0
    for pair in util.logged('labelled_file in', in_file):
        fst, snd = unpack_pair(pair.rstrip())
        while fst != label:
            label = label_file.readline().rstrip()
            assert (label != '')
            index += 1
        out_file.write("%s\n" % pack_pair(snd, index))
    out_file.flush()
    out_file.seek(0)
    util.log('labelled_file', 'finished')
    return out_file

def to_matrix_market(logs):
    """Returns the download graph in the MatrixMarket format"""
    util.log('to_matrix_market', 'starting')

    users = temp_file()
    dois = temp_file()
    edges = temp_file()

    for log in logs:
        # There is honest-to-god unicode in here eg http://www.fileformat.info/info/unicode/char/2013/index.htm
        user = log['si'].encode('utf8')
        doi = log['doi'].encode('utf8')
        users.write('%s\n' % user)
        dois.write('%s\n' % doi)
        edges.write('%s\n' % pack_pair(user, doi))

    users.flush()
    dois.flush()
    edges.flush()

    users = uniq_sorted_file(users)
    dois = uniq_sorted_file(dois)

    edges = labelled_file(uniq_sorted_file(edges), users)
    edges = labelled_file(uniq_sorted_file(edges), dois)

    num_users = count_lines(users)
    num_dois = count_lines(dois)
    num_edges = count_lines(edges)

    mm = temp_file()
    mm.write('%%MatrixMarket matrix coordinate integer general\n')
    mm.write('%i %i %i\n' % (num_users, num_dois, num_edges))
    for pair in edges:
        user_index, doi_index = unpack_pair(pair.rstrip())
        mm.write("%s %s 1\n" % (user_index, doi_index))
    mm.flush()

    util.log('to_matrix_market', 'finished')

    return users, dois, mm

if __name__ == '__main__':
    import itertools
    users, dois, mm = to_matrix_market(from_dump('/mnt/var/Mongo3-backup/LogsRaw-20130113.bson'))
    os.rename(users.name, '/mnt/var/springer-recommendations/users')
    os.rename(dois.name, '/mnt/var/springer-recommendations/dois')
    os.rename(mm.name, '/mnt/var/springer-recommendations/mm')
