"""Preprocess download logs dumped from mongodb"""

import os
import struct
import subprocess
import tempfile
import itertools
import random
import operator
from array import array

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
    subprocess.check_call(['sort', '-T', data_dir, '-S', '80%', '-u', in_stash.name, '-o', out_stash.name])
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

def unnumber(rows, labels, column=0):
    labels = iter(labels)
    label = labels.next()
    index = 0
    for row in rows:
        while index != row[column]:
            label = labels.next()
            index += 1
        row[column] = label

@util.timed
def preprocess(logs):
    util.log('preprocess', 'reading input')
    raw_edges = sorted_stash((log['doi'].encode('utf8'), log['si'].encode('utf8')) for log in logs)

    util.log('preprocess', 'collating')
    raw_dois = sorted_stash((doi for doi, user in raw_edges))
    raw_users = sorted_stash((user for doi, user in raw_edges))

    util.log('preprocess', 'labelling')
    edges = raw_edges
    edges = sorted_stash(((user, doi) for doi, user in numbered(edges, raw_dois)))
    edges = sorted_stash(((doi, user) for user, doi in numbered(edges, raw_users)))

    return raw_dois, raw_users, edges

def minhash(seed, users):
    return min((hash((user, seed)) for user in users))

def jackard_similarity(users1, users2):
    intersection = 0
    difference = 0
    i = 0
    j = 0
    while (i < len(users1)) and (j < len(users2)):
        if users1[i] < users2[j]:
            difference += 1
            i += 1
        elif users1[i] > users2[j]:
            difference += 1
            j += 1
        else:
            intersection += 1
            i += 1
            j += 1
    difference += (len(users1) - i) + (len(users2) - j)
    return float(intersection) / (float(intersection) + float(difference))

@util.timed
def recommendations(edges, num_dois, num_rounds=20, num_recs=5):
    doi2users = [array('I', sorted(((user for _, user in group)))) for doi, group in grouped(edges)]

    doi2scores = array('f', itertools.repeat(0.0, num_dois * num_recs))
    doi2recs = array('i', itertools.repeat(-1, num_dois * num_recs))

    def insert_rec(doi, score, rec):
        for i in xrange(doi * num_recs, (doi + 1) * num_recs):
            if doi2recs[i] == rec:
                break
            elif score > doi2scores[i]:
                doi2scores[i], score = score, doi2scores[i]
                doi2recs[i], rec = rec, doi2recs[i]

    for round in xrange(0, num_rounds):
        util.log('recommendations', 'beginning minhash round %i' % round)
        seed = random.getrandbits(64)
        util.log('recommendations', 'hashing into buckets')
        buckets = [(minhash(seed, users), doi, users) for doi, users in enumerate(doi2users)]
        util.log('recommendations', 'sorting buckets')
        buckets.sort()
        util.log('recommendations', 'checking scores')
        for _, bucket in grouped(buckets):
            bucket = list(bucket)
            random.shuffle(bucket)
            for (_, doi1, users1), (_, doi2, users2) in itertools.izip(bucket, bucket[1:]):
                score = jackard_similarity(users1, users2)
                insert_rec(doi1, score, doi2)
                insert_rec(doi2, score, doi1)

    recs = []
    for doi in xrange(0, num_dois):
        for rec in xrange(0, num_recs):
            i = (doi*num_recs)+rec
            score = doi2scores[i]
            rec = doi2recs[i]
            if score > 0 and rec >= 0:
                recs.append([doi, score, rec])

    return recs

@util.timed
def postprocess(raw_dois, recs):
    recs.sort(key=operator.itemgetter(2))
    unnumber(recs, raw_dois, column=2)
    recs.sort(key=operator.itemgetter(0))
    unnumber(recs, raw_dois, column=0)
    return stash(((doi, [(score, rec) for (_, score, rec) in group]) for doi, group in grouped(recs)))

def main():
    logs = itertools.islice(from_dump('/mnt/var/Mongo3-backup/LogsRaw-20130113.bson'), 1000)
    # logs = itertools.chain(from_dump('/mnt/var/Mongo3-backup/LogsRaw-20130113.bson'), from_dump('/mnt/var/Mongo3-backup/LogsRaw.bson'))
    raw_dois, raw_users, edges = preprocess(logs)
    raw_dois.rename('raw_dois')
    raw_users.rename('raw_users')
    edges.rename('edges')
    num_dois = len(raw_dois)
    recs = recommendations(edges, len(raw_dois))
    raw_recs = postprocess(raw_users, raw_dois, recs)
    raw_recs.rename('raw_recs')

if __name__ == '__main__':
    # import cProfile
    # cProfile.run('main()', 'prof')
    main()
