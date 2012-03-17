"""
Recomendations using item-item cosine similarity. The naive algorithm is very straight forward:

def recommendations(doi_a, downloads, limit):
  scores = []
  for doi_b in dois:
    ips_a = set((download['ip'] for download in downloads if download['doi'] == doi_a))
    ips_b = set((download['ip'] for download in downloads if download['doi'] == doi_b))
    score = len(intersect(ips_a, ips_b)) / len(ips_a) / len(ips_b)
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
    ip_ids = db.Ids(build_name, 'ip')
    doi_ids = db.Ids(build_name, 'doi')
    ip2dois = db.MultiValue(build_name, 'ip2dois')
    doi2ips = db.MultiValue(build_name, 'doi2ips')

    id_struct = db.id_struct

    for _, download in util.notifying_iter(downloads, 'recommendations.collate_downloads'):
        ip = id_struct.pack(ip_ids.get_id(download['ip']))
        doi = id_struct.pack(doi_ids.get_id(download['doi']))
        ip2dois.put(ip, doi)
        doi2ips.put(doi, ip)

    return (ip_ids.next_id, doi_ids.next_id)

id_struct = db.id_struct

def calculate_scores(num_ips, num_dois, build_name, limit=5):
    scores = db.SingleValue(build_name, 'scores')
    ip2dois = [None] * num_ips
    doi2ips = [None] * num_dois

    for doi, ips in util.notifying_iter(db.MultiValue(build_name, 'doi2ips'), "recommendations.calculate_scores(doi2ips)"):
        doi = id_struct.unpack(doi)[0]
        doi2ips[doi] = array.array('L', (id_struct.unpack(ip)[0] for ip in ips))

    for ip, dois in util.notifying_iter(db.MultiValue(build_name, 'ip2dois'), "recommendations.calculate_scores(ip2dois)"):
        ip = id_struct.unpack(ip)[0]
        if len(dois) < settings.max_downloads_per_ip:  # drop the ~0.1% of ips that cause most of the work
            ip2dois[ip] = array.array('L', (id_struct.unpack(doi)[0] for doi in dois))
        else:
            ip2dois[ip] = array.array('L')

    ip_ids = db.Ids(build_name, 'ip')
    doi_ids = db.Ids(build_name, 'doi')

    for doi_a, ips_a in util.notifying_iter(enumerate(doi2ips), "recommendations.calculate_scores(calculate)", interval=1000):
        doi2ips_common = collections.Counter()

        for ip in ips_a:
            for doi in ip2dois[ip]:
                doi2ips_common[doi] += 1

        def scores_a():
            for doi_b, ips_common in doi2ips_common.iteritems():
                if doi_b != doi_a:
                    ips_b = doi2ips[doi_b]
                    score = (ips_common ** 2.0) / len(ips_a) / len(ips_b)
                    yield (score, doi_b)

        top_scores = heapq.nlargest(limit, scores_a(), key=operator.itemgetter(0))
        scores.put(doi_ids.get_string(doi_a), [(score, doi_ids.get_string(doi_b)) for score, doi_b in top_scores])

def build(build_name, limit=5):
    (num_ips, num_dois) = collate_downloads(build_name)
    calculate_scores(num_ips, num_dois, build_name)

# for easy profiling
if __name__ == '__main__':
    (num_ips, num_dois) = collate_downloads(build_name='test')
    print 'Nums', num_ips, num_dois
    calculate_scores(num_ips, num_dois, build_name='test')
