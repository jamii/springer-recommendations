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

def collate(downloads, build_name='test', incremental=False):
    db_ip2dois = db.MultiValue(build_name, 'ip2dois', 'w')
    db_doi2ips = db.MultiValue(build_name, 'doi2ips', 'w')
    dois_modified = set()

    for download in util.notifying_iter(downloads, "recommendations.collate"):
        ip = download['ip']
        doi = download['doi']
        db_ip2dois.put(ip, doi)
        db_doi2ips.put(doi, ip)
        if incremental:
            dois_modified.add(doi)

    db_ip2dois.sync()
    db_doi2ips.sync()

    if incremental:
        return dois_modified
    else:
        return None

def scores(dois_modified=None, build_name='test', limit=5):
    db_ip2dois = db.MultiValue(build_name, 'ip2dois', 'r')
    db_doi2ips = db.MultiValue(build_name, 'doi2ips', 'r')
    db_scores = db.SingleValue(build_name, 'scores', 'w')

    if dois_modified is None:
        doi_iter = db_doi2ips
    else:
        doi_iter = ((doi, doi2ips.get(doi)) for doi in dois_modified)

    for doi_a, ips_a in util.notifying_iter(doi_iter, "recommendations.scores"):

        ip2dois = dict(((ip, db_ip2dois.get(ip)) for ip in ips_a))
        for ip, dois in ip2dois.items():
            if len(dois) >= settings.max_downloads_per_ip: # drop the ~0.1% of ips that cause most of the work
                ip2dois[ip] = []

        dois = set((doi for ip, dois in ip2dois.items() for doi in dois))
        doi2ips = dict(((doi, db_doi2ips.get(doi)) for doi in dois))

        doi2ips_common = collections.Counter()
        for ip in ips_a:
            for doi in ip2dois[ip]:
                doi2ips_common[doi] += 1

        scores = []
        for doi_b, ips_b in doi2ips.items():
            if doi_b != doi_a:
                score = (doi2ips_common[doi_b] ** 2.0) / len(doi2ips[doi_b]) / len(ips_a)
                scores.append((score, doi_b))

        scores = heapq.nlargest(limit, scores)
        db_scores.put(doi_a, scores)

    db_scores.sync()

def build(input, build_name='test', limit=5, incremental=False):
    downloads = from_disco(input)
    dois_modified = collate(downloads, build_name, incremental=incremental)
    # note: if incremental == True then dois_modified == None
    scores(dois_modified, build_name, limit)
