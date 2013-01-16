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

unpack_prefix = struct.Struct('i').unpack

# TODO: might be worth manually decoding the bson here and just picking out si/doi. could also avoid the utf8 encode later.
@util.logged
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

stashes = []

class Stash():
    """Read-only on-disk cache of a list of rows"""
    def __init__(self):
        self.file = tempfile.NamedTemporaryFile(dir=data_dir)
        self.name = self.file.name
        stashes.append(self)
        util.log('stash', self.name)

    @util.logged
    def __iter__(self):
        self.file.seek(0) # always iterate from the start
        return itertools.imap(ujson.loads, self.file)

    def save_as(self, name):
        os.rename(self.file.name, os.path.join(data_dir, name))

@util.logged
def stashed(rows):
    """Store a list of string rows in a temporary file. Assumes row entries never contain \x00 or \n."""
    if isinstance(rows, Stash):
        return rows
    else:
        stash = Stash()
        stash.file.writelines(("%s\n" % ujson.dumps(row) for row in rows))
        stash.file.flush()
        return stash

@util.logged
def uniq_sorted(rows):
    """Return rows sorted by pickle order, with duplicate rows removed"""
    in_stash = stashed(rows)
    out_stash = Stash()
    subprocess.check_call(['sort', '-u', in_stash.name, '-o', out_stash.name])
    return out_stash

def grouped(rows):
    """Return rows grouped by first column"""
    return itertools.groupby(rows, lambda r: r[0])

@util.logged
def edges(logs):
    for log in logs:
        # There is honest-to-god unicode in here eg http://www.fileformat.info/info/unicode/char/2013/index.htm
        doi = log['doi'].encode('utf8')
        user = log['si'].encode('utf8')
        yield doi, user

@util.logged
def doi_rows(edges):
    for doi, rows in grouped(uniq_sorted(edges)):
        users = [row[1] for row in rows]
        yield doi, users

@util.logged
def min_hashes(doi_rows):
    """Minhash approximation as described by Das, Abhinandan S., et al. "Google news personalization: scalable online collaborative filtering." Proceedings of the 16th international conference on World Wide Web. ACM, 2007. """
    seed = random.getrandbits(64)
    for doi, users in doi_rows:
        hashes = [hash((seed, user)) for user in users]
        yield min(hashes), doi, users

def pairs(xs):
    for i, x1 in enumerate(xs):
        for x2 in xs[(i+1):]:
            yield x1, x2

def jaccard_similarity(users1, users2):
    return float(len(users1.intersection(users2))) / float(len(users1.union(users2)))

@util.logged
def scores(min_hashes):
    for min_hash, group in grouped(uniq_sorted(min_hashes)):
        bucket = [(doi, set(users)) for (_, doi, users) in group]
        for (doi1, users1), (doi2, users2) in pairs(bucket):
            score = jaccard_similarity(users1, users2)
            yield doi1, doi2, score
            yield doi2, doi1, score

@util.logged
def recommendations(logs, iterations=1, top_n=5):
    doi_rows_stash = stashed(doi_rows(edges(logs)))
    scores_iter = (scores(min_hashes(doi_rows_stash)) for _ in xrange(0, iterations))
    scores_stash = stashed(itertools.chain.from_iterable(scores_iter))
    for doi1, group in grouped(uniq_sorted(scores_stash)):
        top_scores = heapq.nlargest(top_n, itertools.imap(operator.itemgetter(2,1), group))
        yield doi1, top_scores

if __name__ == '__main__':
    try:
        logs = itertools.islice(from_dump('/mnt/var/Mongo3-backup/LogsRaw-20130113.bson'), 1000000)
        recs = recommendations(logs)
        stashed(recs).save_as('recs')
    finally:
        raw_input("Die?")
