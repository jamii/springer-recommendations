import collections
import heapq
import matplotlib.pyplot as plot

import db

def value_lengths(iter):
    counter = collections.Counter()

    for key, values in iter:
        counter[len(values)] += 1

    for i in xrange(0, max(counter.keys()) + 1):
        yield counter[i]

def value_extremes(iter, n, unpack=lambda x: x):
    def lengths():
        for key, values in iter:
            yield len(values), key

    largest = heapq.nlargest(n, lengths())

    return [(unpack(key), value) for value, key in largest]

def plot_doi_si(build_name):
    plot.subplot(121)
    doi2sis = db.MultiValue(build_name, 'doi2sis')
    doi_ids = db.Ids(build_name, 'doi')
    def unpack(struct):
        id = db.id_struct.unpack(struct)[0]
        return doi_ids.get_string(id)
    print value_extremes(doi2sis, 25, unpack=unpack)
    plot.loglog(list(value_lengths(doi2sis)))
    plot.xlabel('Number of sis downloading')
    plot.ylabel('Frequency')

    plot.subplot(122)
    si2dois = db.MultiValue(build_name, 'si2dois')
    si_ids = db.Ids(build_name, 'si')
    def unpack(struct):
        id = db.id_struct.unpack(struct)[0]
        return si_ids.get_string(id)
    print value_extremes(si2dois, 25, unpack=unpack)
    plot.loglog(list(value_lengths(si2dois)))
    plot.xlabel('Number of dois downloaded')
    plot.ylabel('Frequency')

    plot.show()
