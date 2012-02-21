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

import db
import util
import settings

def assign_ids(build_name='test'):
    downloads = db.SingleValue(build_name, 'downloads')
    ip_ids = db.Ids(build_name, 'ip')
    doi_ids = db.Ids(build_name, 'doi')

    next_ip_id = 0
    next_doi_id = 0

    for _, download in util.notifying_iter(downloads, 'recommendations.assign_ids'):
        ip_id = ip_ids.get_id(download['ip'])
        doi_id = doi_ids.get_id(download['doi'])
        yield (ip_id, doi_id)

def calculate_scores(limit=5, build_name='test'):
    ip2dois = collections.defaultdict(set)
    doi2ips = collections.defaultdict(set)
    scores = db.SingleValue(build_name, 'scores')

    for ip, doi in util.notifying_iter(assign_ids(build_name), "recommendations.calculate_scores(collate)"):
        ip2dois[ip].add(doi)
        doi2ips[doi].add(ip)

    ip_ids = db.Ids(build_name, 'ip')
    doi_ids = db.Ids(build_name, 'doi')

    for doi_a, ips_a in util.notifying_iter(doi2ips.iteritems(), "recommendations.calculate_scores(calculate)", interval=1000):
        doi2ips_common = collections.Counter()

        for ip in ips_a:
            dois = ip2dois.get(ip)
            if len(dois) < settings.max_downloads_per_ip:  # drop the ~0.1% of ips that cause most of the work
                for doi in dois:
                    doi2ips_common[doi] += 1

        def scores_a():
            for doi_b, ips_common in doi2ips_common.iteritems():
                if doi_b != doi_a:
                    ips_b = doi2ips.get(doi_b)
                    score = (ips_common ** 2.0) / len(ips_a) / len(ips_b)
                    yield (score, doi_b)

        top_scores = heapq.nlargest(limit, scores_a())
        scores.put(doi_ids.get_string(doi_a), [(score, doi_ids.get_string(doi_b)) for score, doi_b in top_scores])

def build(limit=5, build_name='test'):
    calculate_scores(limit, build_name)

# for easy profiling
if __name__ == '__main__':
    calculate_scores(build_name='test')
