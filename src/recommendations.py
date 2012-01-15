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

Unfortunately this does not scale so well when we have 5 million dois and 1 billion downloads. The code below performs the same calculations using only on-disk sorting and sequential reads/writes.

Classes are named after the output they produce eg Doi2Ips yields (doi, [ip1, ip2, ...]
"""

import json

import mr

class Ip2Dois(mr.Job):
    # input from ParseDownloads

    @staticmethod
    @mr.map_with_errors
    def map((id, download), params):
        yield download['ip'], download['doi']

    sort = True
    reduce = staticmethod(mr.group_uniq)

class Doi2Dois(mr.Job):
    # input from Ip2Dois

    @staticmethod
    def map((ip, dois), params):
        for doi_a in dois:
            for doi_b in dois:
                if doi_a != doi_b:
                    yield doi_a, doi_b

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

class Doi2DoisWithIps(mr.Job):
    # input from mr.zip(Doi2Dois, Doi2Ips)
    map_reader = staticmethod(mr.zip_reader)

    @staticmethod
    def map((doi_a, (dois, ips_a)), params):
        for doi_b in dois:
            yield (doi_b, (doi_a, ips_a))

    sort = True
    reduce = staticmethod(mr.group) # values should already be unique

class Scores(mr.Job):
    # input from mr.zip(Doi2DoisWithIps, Doi2Ips)
    map_reader = staticmethod(mr.zip_reader)

    @staticmethod
    def map((doi_b, (dois_with_ips, ips_b)), params):
        scores = [(score(ips_a, ips_b), doi_a) for (doi_a, ips_a) in dois_with_ips]
        scores.sort()
        yield doi_b, scores[:params['limit']]

def build(downloads, directory='./recommendations', limit=5):
    ip2dois = Ip2Dois().run(input=downloads.wait())
    mr.print_errors(ip2dois)

    doi2dois = Doi2Dois().run(input=ip2dois.wait())
    mr.print_errors(doi2dois)

    ip2dois.purge()

    doi2ips = Doi2Ips().run(input=downloads.wait())
    mr.print_errors(doi2ips)

    doi2dois_with_ips = Doi2DoisWithIps().run(input=mr.zip(doi2dois.wait(), doi2ips.wait()))
    mr.print_errors(doi2dois_with_ips)

    doi2dois.purge()

    scores = Scores().run(
        input=mr.zip(doi2dois_with_ips.wait(), doi2ips.wait()),
        params={'limit':5}
        )

    doi2ips.purge()
    doi2dois_with_ips.purge()

    mr.write_results(scores, directory, json.dumps)

    scores.purge()
