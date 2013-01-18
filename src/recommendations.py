"""Preprocess download logs dumped from mongodb"""

import os
import struct
import subprocess
import tempfile
import itertools
import random
import heapq
import operator

import bson
import ujson

import util

data_dir = "/mnt/var/springer-recommendations/"

max_downloads_per_user = 1000

unpack_prefix = struct.Struct('i').unpack

# TODO: might be worth manually decoding the bson here and just picking out si/doi. could also avoid the utf8 encode later.
def from_dump(dump_filename):
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

# have to keep an explicit reference to the stashes because many itertools constructs don't
stashes = []

class stash():
    """On-disk cache of a list of rows"""
    def __init__(self, rows=[]):
        stashes.append(self)
        self.file = tempfile.NamedTemporaryFile(dir=data_dir)
        self.name = self.file.name
        dumps = ujson.dumps # don't want to do this lookup inside the loop below
        self.file.writelines(("%s\n" % dumps(row) for row in rows))
        self.file.flush()

    def __iter__(self):
        self.file.seek(0) # always iterate from the start
        return itertools.imap(ujson.loads, self.file)

    def __len__(self):
        result = subprocess.check_output(['wc', '-l', self.file.name])
        count, _ = result.split()
        return int(count)

    def rename(self, name):
        full_name = os.path.join(data_dir, name)
        os.rename(self.file.name, full_name)
        self.file = open(full_name)
        self.name = full_name

def sorted_stash(rows):
    in_stash = stash(rows)
    out_stash = stash()
    subprocess.check_call(['sort', '-T', data_dir, '-u', in_stash.name, '-o', out_stash.name])
    return out_stash

def grouped(rows):
    return itertools.groupby(rows, operator.itemgetter(0))

def numbered(rows, labels):
    labels = iter(labels)
    label = labels.next()
    index = 0
    for row in rows:
        while label != row[0]:
            label = labels.next()
            index += 1
        row[0] = index
        yield row

def unnumbered(rows, labels):
    labels = iter(labels)
    label = labels.next()
    index = 0
    for row in rows:
        while index != row[0]:
            label = labels.next()
            index += 1
        row[0] = label
        yield row

@util.timed
def preprocess(logs):
    raw_edges = sorted_stash((log['doi'].encode('utf8'), log['si'].encode('utf8')) for log in logs)
    raw_dois = sorted_stash((doi for doi, user in raw_edges))
    raw_users = sorted_stash((user for doi, user in raw_edges))

    # num_dois, num_users = len(raw_dois), len(raw_users)

    edges = raw_edges
    edges = sorted_stash(((user, doi) for doi, user in numbered(edges, raw_dois)))
    edges = sorted_stash(((doi, user) for user, doi in numbered(edges, raw_users)))

    return raw_dois, raw_users, edges

def minhash(seed, users):
    return min((hash((seed, user)) for user in users))

@util.timed
def recommendations(edges, num_iters=1, num_recs=5):
    doi2users = [(doi, set((user for _, user in group))) for doi, group in grouped(edges)]
    doi2recs = [[(0,None)] * num_recs for doi, _ in grouped(edges)]

    for _ in xrange(0, num_iters):
        seed = random.getrandbits(64)
        buckets = [(minhash(seed, users), doi, users) for doi, users in doi2users]
        buckets.sort()
        for _, bucket in grouped(buckets):
            for (_, doi1, users1), (_, doi2, users2) in itertools.combinations(bucket, 2):
                score = float(len(users1.intersection(users2))) / float(len(users1.union(users2)))
                heapq.heappushpop(doi2recs[doi1], (score, doi2))
                heapq.heappushpop(doi2recs[doi2], (score, doi1))

    return doi2recs

@util.timed
def postprocess(raw_users, raw_dois, doi2recs):
    return stash(doi2recs)

def main():
    logs = itertools.islice(from_dump('/mnt/var/Mongo3-backup/LogsRaw-20130113.bson'), 1000000)
    raw_users, raw_dois, edges = preprocess(logs)
    edges.rename('edges')
    doi2recs = recommendations(edges)
    raw_doi2recs = postprocess(raw_users, raw_dois, doi2recs)
    raw_doi2recs.rename('recs')

if __name__ == '__main__':
    # import cProfile
    # cProfile.run('main()', 'prof')
    main()
