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

import mr
import db

class Ip2Dois(mr.Job):
    # input from ParseDownloads

    @staticmethod
    @mr.map_with_errors
    def map((id, download), params):
        yield download['ip'], download['doi']

    sort = True
    reduce = staticmethod(mr.group_uniq)

class Doi2Ips(mr.Job):
    # input from ParseDownloads

    @staticmethod
    @mr.map_with_errors
    def map((id, download), params):
        yield download['doi'], download['ip']

    sort = True
    reduce = staticmethod(mr.group_uniq)

class Scores(mr.Job):
    # input from Doi2Ips

    status_interval = 100

    @staticmethod
    def map_init(iter, params):
        params['ip2dois'] = db.DB('recommendations', 'ip2dois')
        params['doi2ips'] = db.DB('recommendations', 'doi2ips')

    @staticmethod
    def map((doi_a, ips_a), params):
        ip2dois = params['ip2dois'].get_multi(ips_a)
        dois = list(set((doi for dois in ip2dois.values() for doi in dois)))
        doi2ips = params['doi2ips'].get_multi(dois)

        doi2ips_common = {}
        for ip in ips_a:
            for doi in ip2dois[ip]:
                doi2ips_common[doi] = doi2ips_common.get(doi, 0) + 1

        scores = []
        for doi_b, ips_b in doi2ips.items():
            score = float(doi2ips_common[doi_b]) / len(doi2ips[doi_b]) / len(ips_a)
            scores.append((score, doi_b))
        scores.sort(reverse=True)

        yield doi_a, scores[:params['limit']]

def build(downloads, directory='recommendations', limit=5):
    ip2dois = Ip2Dois().run(input=downloads)
    mr.print_errors(ip2dois)
    db.insert(ip2dois.wait(), 'recommendations', 'ip2dois')
    ip2dois.purge()

    doi2ips = Doi2Ips().run(input=downloads)
    mr.print_errors(doi2ips)
    db.insert(doi2ips.wait(), 'recommendations', 'doi2ips')

    scores = Scores().run(input=doi2ips.wait(), params={'limit':5})
    mr.print_errors(scores)

    doi2ips.purge()

    mr.write_results(scores.wait(), directory, json.dumps)

    scores.purge()
