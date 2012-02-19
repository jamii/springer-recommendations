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

import disco.core

import db
import util
import settings

def from_disco(input):
    for _, value in disco.core.result_iterator(input):
        yield value

def collate_downloads(build_name='test'):
    downloads = db.SingleValue(build_name, 'downloads', 'r')
    ip2dois = db.MultiValue(build_name, 'ip2dois', 'w')
    doi2ips = db.MultiValue(build_name, 'doi2ips', 'w')

    for _, download in util.notifying_iter(downloads, "recommendations.collate_downloads", interval=10000):
        ip = download['ip']
        doi = download['doi']
        ip2dois.put(ip, doi)
        doi2ips.put(doi, ip)

    ip2dois.sync()
    doi2ips.sync()

def calculate_scores(limit=5, build_name='test'):
    ip2dois = db.MultiValue(build_name, 'ip2dois', 'r')
    doi2ips = db.MultiValue(build_name, 'doi2ips', 'r')
    scores = db.SingleValue(build_name, 'scores', 'w')

    for doi_a, ips_a in util.notifying_iter(doi2ips, "recommendations.calculate_scores", interval=1000):
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

        scores.put(doi_a, heapq.nlargest(limit, scores_a()))

    scores.sync()

def build(build_name='test', limit=5):
    collate_downloads(build_name)
    calculate_scores(limit, build_name)
