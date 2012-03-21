"""
Recomendations using item-item cosine similarity. The naive algorithm is very straight forward:

def recommendations(doi_a, downloads, limit):
  scores = []
  for doi_b in dois:
    sis_a = set((download['si'] for download in downloads if download['doi'] == doi_a))
    sis_b = set((download['si'] for download in downloads if download['doi'] == doi_b))
    score = len(intersect(sis_a, sis_b)) / len(sis_a) / len(sis_b)
    scores.append((score, doi_b))
  return sorted(scores)[:limit]

Unfortunately this does not scale so well when we have 5 million dois and 1 billion downloads.
"""

import json
import heapq
import collections
import operator
import array

import db
import util
import settings

def collate_downloads(build_name):
    downloads = db.SingleValue(build_name, 'downloads')
    si_ids = db.Ids(build_name, 'si')
    doi_ids = db.Ids(build_name, 'doi')
    si2dois = db.MultiValue(build_name, 'si2dois')
    doi2sis = db.MultiValue(build_name, 'doi2sis')

    id_struct = db.id_struct

    for _, download in util.notifying_iter(downloads, 'recommendations.collate_downloads'):
        si = id_struct.pack(si_ids.get_id(download['si']))
        doi = id_struct.pack(doi_ids.get_id(download['doi']))
        si2dois.put(si, doi)
        doi2sis.put(doi, si)

    return (si_ids.next_id, doi_ids.next_id)

id_struct = db.id_struct

def calculate_scores(num_sis, num_dois, build_name, limit=5):
    scores = db.SingleValue(build_name, 'scores')
    si2dois = [None] * num_sis
    doi2sis = [None] * num_dois

    for doi, sis in util.notifying_iter(db.MultiValue(build_name, 'doi2sis'), "recommendations.calculate_scores(doi2sis)"):
        doi = id_struct.unpack(doi)[0]
        doi2sis[doi] = array.array('L', (id_struct.unpack(si)[0] for si in sis))

    for si, dois in util.notifying_iter(db.MultiValue(build_name, 'si2dois'), "recommendations.calculate_scores(si2dois)"):
        si = id_struct.unpack(si)[0]
        si2dois[si] = array.array('L', (id_struct.unpack(doi)[0] for doi in dois))

    si_ids = db.Ids(build_name, 'si')
    doi_ids = db.Ids(build_name, 'doi')

    for doi_a, sis_a in util.notifying_iter(enumerate(doi2sis), "recommendations.calculate_scores(calculate)", interval=1000):
        doi2sis_common = collections.Counter()

        for si in sis_a:
            for doi in si2dois[si]:
                doi2sis_common[doi] += 1

        def scores_a():
            for doi_b, sis_common in doi2sis_common.iteritems():
                if doi_b != doi_a:
                    sis_b = doi2sis[doi_b]
                    score = (sis_common ** 2.0) / len(sis_a) / len(sis_b)
                    yield (score, doi_b)

        top_scores = heapq.nlargest(limit, scores_a(), key=operator.itemgetter(0))
        scores.put(doi_ids.get_string(doi_a), [(score, doi_ids.get_string(doi_b)) for score, doi_b in top_scores])

def build(build_name, limit=5):
    (num_sis, num_dois) = collate_downloads(build_name)
    calculate_scores(num_sis, num_dois, build_name)

# for easy profiling
if __name__ == '__main__':
    (num_sis, num_dois) = collate_downloads(build_name='test')
    print 'Nums', num_sis, num_dois
    calculate_scores(num_sis, num_dois, build_name='test')
