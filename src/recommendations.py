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

    status_interval = 100
    partitions = 64

    @staticmethod
    def map((id, download), params):
        if download['ip'] and download['doi']:
            yield download['ip'], download['doi']

    sort = True

    @staticmethod
    def reduce(iter, params):
        total_counter = collections.Counter()
        common_counter = collections.defaultdict(collections.Counter)
        for ip, dois in disco.util.kvgroup(iter):
            dois = list(dois) # dois is an exhaustible iter :(
            for doi_a in dois:
                total_counter[doi_a] += 1
                for doi_b in dois:
                    if doi_a != doi_b:
                        common_counter[doi_a][doi_b] += 1
                        common_counter[doi_b][doi_a] += 1

        for doi_a, partial_total in total_counter.iteritems():
            partial_common = common_counter[doi_a]
            partial_totals = dict(((doi_b, total_counter[doi_b]) for doi_b in partial_common.iterkeys()))
            yield doi_a, (partial_total, partial_common, partial_totals)

class MergeScores(mr.Job):
    # input from PartialScores

    status_interval = 100
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
