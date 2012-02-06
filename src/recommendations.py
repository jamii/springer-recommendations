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

import mr
import db

class Ip2Dois(mr.Job):
    # input from FetchDownloads

    @staticmethod
    def map((id, download), params):
        if download['ip'] and download['doi']:
            yield download['ip'], download['doi']

    sort = True
    reduce = staticmethod(mr.group_uniq)

class Doi2Ips(mr.Job):
    # input from ParseDownloads

    @staticmethod
    def map((id, download), params):
        if download['doi'] and download['ip']:
            yield download['doi'], download['ip']

    sort = True
    reduce = staticmethod(mr.group_uniq)

class Scores(mr.Job):
    # input from Doi2Ips

    @staticmethod
    def map_init(iter, params):
        params['ip2dois'] = db.DB(params['db_name'], 'ip2dois')
        params['doi2ips'] = db.DB(params['db_name'], 'doi2ips')

    @staticmethod
    def map((doi_a, ips_a), params):
        ip2dois = params['ip2dois'].get_multi(ips_a)
        for ip, dois in ip2dois.items():
            if len(dois) >= 1000:
                ip2dois[ip] = []
        dois = list(set((doi for ip, dois in ip2dois.items() for doi in dois)))
        doi2ips = params['doi2ips'].get_multi(dois)

        doi2ips_common = collections.Counter()
        for ip in ips_a:
            for doi in ip2dois[ip]:
                doi2ips_common[doi] += 1

        scores = []
        for doi_b, ips_b in doi2ips.items():
            if doi_b != doi_a:
                score = (doi2ips_common[doi_b] ** 2.0) / len(doi2ips[doi_b]) / len(ips_a)
                scores.append((score, doi_b))

        yield doi_a, heapq.nlargest(params['limit'], scores)

def db_name(build_name):
    return 'recommendations-' + build_name

def build(downloads, build_name='test', limit=5):
    ip2dois = Ip2Dois().run(input=downloads)
    db.insert(ip2dois.wait(), db_name(build_name), 'ip2dois')
    ip2dois.purge()

    doi2ips = Doi2Ips().run(input=downloads)
    db.insert(doi2ips.wait(), db_name(build_name), 'doi2ips')

    scores = Scores().run(input=doi2ips.wait(), params={'limit':5, 'db_name':db_name(build_name)})
    mr.print_errors(scores)

    doi2ips.purge()

    mr.write_results(scores.wait(), build_name, 'recommendations', json.dumps)

    scores.purge()
