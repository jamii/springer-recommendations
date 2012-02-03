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

import collections
import heapq
import json

import disco.util

import mr
import db

def score(total_a, common, total_b):
    """Cosine distance"""
    return (common ** 2) / total_a / total_b

class PartialScores(mr.Job):
    # input from FetchDownloads

    partitions = 64

    @staticmethod
    def map((id, download), params):
        if download['ip'] and download['doi']:
            yield download['ip'], download['doi']

    sort = True

    @staticmethod
    def reduce(iter, params):
        ip2dois = collections.defaultdict(set)
        doi2ips = collections.defaultdict(set)

        for ip, doi in iter:
           ip2dois[ip].add(doi)
           doi2ips[doi].add(ip)

        for doi_a, ips in doi2ips.iteritems():
            total = len(doi2ips[doi_a])
            common_counter = collections.Counter((doi_b for ip in doi2ips[doi_a] for doi_b in ip2dois[ip]))
            total_counter = dict((doi_b, len(doi2ips[doi_b])) for doi_b  in common_counter.iterkeys())
            yield doi_a, (total, common_counter, total_counter)

class MergeScores(mr.Job):
    # input from PartialScores

    status_interval = 1000
    partitions = 64

    sort = True

    @staticmethod
    def reduce(iter, params):
        for doi_a, counts in disco.util.kvgroup(iter):
            total = 0
            common = collections.Counter()
            totals = collections.Counter()
            for (partial_total, partial_common, partial_totals) in counts:
                total += partial_total
                common.update(partial_common)
                totals.update(partial_totals)
            scores = ((score(total, common[doi_b], totals[doi_b]), doi_b) for doi_b in common.iterkeys())
            yield doi_a, heapq.nlargest(params['limit'], scores)

def build(downloads, build_name='test', limit=5):
    partial_scores = PartialScores().run(input=downloads)
    merge_scores = MergeScores().run(input=partial_scores.wait(), params={'limit':5})

    mr.write_results(merge_scores.wait(), build_name, 'recommendations', json.dumps)

    partial_scores.purge()
    merge_scores.purge()
