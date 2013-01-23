"""Fast, scalable item-item recommendations based on Das, Abhinandan S., et al. "Google news personalization: scalable online collaborative filtering." Proceedings of the 16th international conference on World Wide Web. ACM, 2007."""

import os
import sys
import shutil
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
import settings

unpack_prefix = struct.Struct('i').unpack

def from_dump(dump_filename):
    """Read a mongodb dump containing bson-encoded download logs"""
    # TODO: might be worth manually decoding the bson here and just picking out si/doi. could also avoid the utf8 encode later.
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
            log = bson.BSON(data).decode()
            user = log.get('si', '') or log.get('ip', '')
            doi = log.get('doi', '')
            if user and doi:
                yield user.encode('utf8'), doi.encode('utf8')

# have to keep an explicit reference to the stashes because many itertools constructs don't
stashes = []

class stash():
    """On-disk cache of a list of rows"""
    def __init__(self, rows=[]):
        stashes.append(self)
        self.file = tempfile.NamedTemporaryFile(dir=settings.data_dir)
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

    def save_as(self, name):
        shutil.copy(self.file.name, os.path.join(settings.data_dir, name))

def sorted_stash(rows):
    """A sorted, de-duped stash"""
    if isinstance(rows, stash):
        in_stash = rows
    else:
        in_stash = stash(rows)
    out_stash = stash()
    subprocess.check_call(['sort', '-T', settings.data_dir, '-S', '80%', '-u', in_stash.name, '-o', out_stash.name])
    return out_stash

def grouped(rows):
    """Group rows by their first column"""
    return itertools.groupby(rows, operator.itemgetter(0))

def numbered(rows, labels):
    """For each row, replace the first column by its index in labels. Assumes both rows and labels are sorted. Returns a new iter."""
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
    """For each row, lookup column as an index in labels. Assumes both rows and labels are sorted. Modifies rows in place."""
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
    """Replace string DOIs and users by integer indices for more compact representation later"""
    util.log('preprocess', 'reading logs')
    raw_edges = stash(logs)

    util.log('preprocess', 'collating')
    raw_users = sorted_stash((user for user, doi in raw_edges))
    raw_dois = sorted_stash((doi for user, doi in raw_edges))

    util.log('preprocess', 'labelling')
    edges = raw_edges
    edges = numbered(sorted_stash(edges), raw_users)
    edges = ((doi, user) for user, doi in edges)
    edges = numbered(sorted_stash(edges), raw_dois)
    edges = stash(edges)

    return raw_dois, edges

def minhash(seed, users):
    """The minimum hash of a list of users. See http://en.wikipedia.org/wiki/MinHash"""
    return min((hash((user, seed)) for user in users))

def jaccard_similarity(users1, users2):
    """Jaccard similarity between two sets represented as sorted arrays of integers. See http://en.wikipedia.org/wiki/Jaccard_index"""
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
def recommendations(edges, num_dois):
    """For each doi in edges, try to find the nearest settings.recommendations_per_doi DOIs by Jaccard similarity using minhashing"""
    doi2users = [array('I', sorted(((user for _, user in group)))) for doi, group in grouped(edges)]

    # store top settings.recommmendations_per_doi recs for each doi in two huge arrays, to save on object overhead
    doi2scores = array('f', itertools.repeat(0.0, num_dois * settings.recommendations_per_doi))
    doi2recs = array('i', itertools.repeat(-1, num_dois * settings.recommendations_per_doi))

    def insert_rec(doi, score, rec):
        for i in xrange(doi * settings.recommendations_per_doi, (doi + 1) * settings.recommendations_per_doi):
            if doi2recs[i] == rec:
                break
            elif score > doi2scores[i]:
                doi2scores[i], score = score, doi2scores[i]
                doi2recs[i], rec = rec, doi2recs[i]

    for round in xrange(0, settings.minhash_rounds):
        util.log('recommendations', 'beginning minhash round %i' % round)
        seed = random.getrandbits(64)
        util.log('recommendations', 'hashing into buckets')
        buckets = [(minhash(seed, users), doi, users) for doi, users in enumerate(doi2users)]
        util.log('recommendations', 'sorting buckets')
        buckets.sort()
        util.log('recommendations', 'checking scores')
        for _, bucket in grouped(buckets):
            bucket = list(bucket)
            random.shuffle(bucket) # shuffle bucket because it's currently in doi order
            for (_, doi1, users1), (_, doi2, users2) in itertools.izip(bucket, bucket[1:]):
                score = jaccard_similarity(users1, users2)
                insert_rec(doi1, score, doi2)
                insert_rec(doi2, score, doi1)

    recs = []
    for doi in xrange(0, num_dois):
        for rec in xrange(0, settings.recommendations_per_doi):
            i = (doi*settings.recommendations_per_doi)+rec
            score = doi2scores[i]
            rec = doi2recs[i]
            if score > 0 and rec >= 0:
                recs.append([doi, score, rec])

    return recs

@util.timed
def postprocess(raw_dois, recs):
    """Turn integer DOIs and users back into strings"""
    recs.sort(key=operator.itemgetter(2))
    unnumber(recs, raw_dois, column=2)
    recs.sort(key=operator.itemgetter(0))
    unnumber(recs, raw_dois, column=0)
    return stash(((doi, [(score, rec) for (_, score, rec) in group]) for doi, group in grouped(recs)))

def main():
    logs = itertools.chain.from_iterable((from_dump(dump_filename.rstrip()) for dump_filename in sys.stdin.readlines()))
    # logs = itertools.islice(logs, 1000) # for quick testing
    raw_dois, edges = preprocess(logs)
    util.log('main', '%i unique edges' % len(edges))
    recs = recommendations(edges, len(raw_dois))
    raw_recs = postprocess(raw_dois, recs)
    sys.stdout.writelines(("%s\n" % ujson.dumps(row) for row in raw_recs))
    sys.stdout.flush()

if __name__ == '__main__':
    # import cProfile
    # cProfile.run('main()', 'prof')
    main()
