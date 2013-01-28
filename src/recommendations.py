"""Fast, scalable item-item recommendations based on Das, Abhinandan S., et al. "Google news personalization: scalable online collaborative filtering." Proceedings of the 16th international conference on World Wide Web. ACM, 2007."""

import os
import sys
import shutil
import subprocess
import tempfile
import itertools
import random
import operator
from array import array

import ujson

import util
import settings

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

    @staticmethod
    def sorted(rows):
        """A sorted, de-duped stash"""
        if isinstance(rows, stash):
            in_stash = rows
        else:
            in_stash = stash(rows)
        out_stash = stash()
        subprocess.check_call(['sort', '-T', settings.data_dir, '-S', '80%', '-u', in_stash.name, '-o', out_stash.name])
        return out_stash

    @staticmethod
    def from_file(file):
        out_stash = stash()
        out_stash.file = file
        return out_stash

    def __iter__(self):
        self.file.seek(0) # always iterate from the start
        return itertools.imap(ujson.loads, self.file)

    def __len__(self):
        result = subprocess.check_output(['wc', '-l', self.file.name])
        count, _ = result.split()
        return int(count)

    def save_as(self, name):
        shutil.copy(self.file.name, os.path.join(settings.data_dir, name))

class priority_queues():
    """A number of fixed-size priority queues packed together for low memory usage"""
    def __init__(self, num_queues, max_size):
        self.num_queues = num_queues
        self.max_size = max_size
        self.entries = array('f', itertools.repeat(-1, num_queues * max_size))
        self.priorities = array('f', itertools.repeat(0.0, num_queues * max_size))

    def insert(self, queue_index, entry, priority):
        """Insert an entry into the queue at the given index. Requires entry >= 0, priority >= 0.0"""
        for i in xrange(queue_index * self.max_size, (queue_index + 1) * self.max_size):
            if self.entries[i] == entry:
                break
            elif priority > self.priorities[i]:
                self.entries[i], entry = entry, self.entries[i]
                self.priorities[i], priority = priority, self.priorities[i]

    def __iter__(self):
        for queue_index in xrange(0, self.num_queues):
            for entry_index in xrange(0, self.max_size):
                i = (queue_index * self.max_size) + entry_index
                entry = self.entries[i]
                priority = self.priorities[i]
                if entry >= 0 and priority > 0:
                    yield [queue_index, entry, priority]

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
def preprocess(raw_edges):
    """Replace string DOIs and users by integer indices for more compact representation later"""
    util.log('preprocess', 'copying input')
    raw_edges = stash(raw_edges)

    util.log('preprocess', 'collating')
    raw_users = stash.sorted((user for user, doi in raw_edges))
    raw_dois = stash.sorted((doi for user, doi in raw_edges))

    util.log('preprocess', 'labelling')
    edges = raw_edges
    edges = numbered(stash.sorted(edges), raw_users)
    edges = ((doi, user) for user, doi in edges)
    edges = numbered(stash.sorted(edges), raw_dois)
    edges = stash(edges)

    return raw_dois, edges

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
def minhash_round(buckets):
    """Probabalistic algorithm for finding edges with high jaccard scores (see http://en.wikipedia.org/wiki/MinHash). Modifies buckets in-place."""
    seed = random.getrandbits(64)
    util.log('minhash_round', 'hashing into buckets')
    for bucket in buckets:
        users = bucket[3]
        bucket[0] = min((hash((seed, user, seed)) for user in users)) # minhash
        bucket[1] = random.random() # prevents bias towards adjacent dois caused by sorting
    util.log('minhash_round', 'sorting buckets')
    buckets.sort()
    util.log('minhash_round', 'checking scores')
    for (_, _, doi1, users1), (_, _, doi2, users2) in itertools.izip(buckets, buckets[1:]):
        score = jaccard_similarity(users1, users2)
        yield doi1, doi2, score

@util.timed
def recommendations(edges, num_dois):
    """For each doi in edges, try to find the nearest settings.recommendations_per_doi DOIs by Jaccard similarity using minhashing"""
    # list of (minhash, random, user, doi)
    buckets = [[0, 0, doi, array('I', sorted(((user for _, user in group))))] for doi, group in grouped(edges)]

    # store a priority queue of recommendations per doi
    recs = priority_queues(num_queues = num_dois, max_size = settings.recommendations_per_doi)

    for round in xrange(0, settings.minhash_rounds):
        for doi1, doi2, score in minhash_round(buckets):
            recs.insert(doi1, doi2, score)
            recs.insert(doi2, doi1, score)

    return recs

@util.timed
def postprocess(raw_dois, recs):
    """Turn integer DOIs and users back into strings"""
    recs.sort(key=operator.itemgetter(1))
    unnumber(recs, raw_dois, column=1)
    recs.sort(key=operator.itemgetter(0))
    unnumber(recs, raw_dois, column=0)
    return stash(((doi, [(score, rec) for (_, score, rec) in group]) for doi, group in grouped(recs)))

def main():
    raw_edges = itertools.chain.from_iterable((stash.from_file(open(dump_filename.rstrip())) for dump_filename in sys.stdin.readlines()))
    # raw_edges = itertools.islice(raw_edges, 1000) # for quick testing
    raw_dois, edges = preprocess(raw_edges)
    util.log('main', '%i unique edges' % len(edges))
    recs = list(recommendations(edges, len(raw_dois)))
    raw_recs = postprocess(raw_dois, recs)
    sys.stdout.writelines(("%s\n" % ujson.dumps(row) for row in raw_recs))
    sys.stdout.flush()

if __name__ == '__main__':
    # import cProfile
    # cProfile.run('main()', 'prof')
    main()
