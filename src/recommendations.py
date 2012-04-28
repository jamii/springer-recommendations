"""
Recomendations using item-item cosine similarity. The naive algorithm is very straight forward:

def recommendations(doi_a, downloads, limit):
  scores = []
  for doi_b in dois:
    users_a = set((download['user'] for download in downloads if download['doi'] == doi_a))
    users_b = set((download['user'] for download in downloads if download['doi'] == doi_b))
    score = len(intersect(users_a, users_b)) / len(users_a) / len(users_b)
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

id_struct = db.id_struct

def collate_downloads(build_name):
    downloads = db.SingleValue(build_name, 'downloads')
    user_ids = db.Ids(build_name, 'user')
    doi_ids = db.Ids(build_name, 'doi')
    user2dois = db.MultiValue(build_name, 'user2dois')
    doi2users = db.MultiValue(build_name, 'doi2users')

    for _, download in util.notifying_iter(downloads, 'recommendations.collate_downloads'):
        user = id_struct.pack(user_ids.get_id(download.get('si', None) or download['ip']))
        doi = id_struct.pack(doi_ids.get_id(download['doi']))
        user2dois.put(user, doi)
        doi2users.put(doi, user)

    return (user_ids.next_id, doi_ids.next_id)

def calculate_scores(num_users, num_dois, build_name, limit=5):
    scores = db.SingleValue(build_name, 'scores')
    user2dois = [None] * num_users
    doi2users = [None] * num_dois

    for doi, users in util.notifying_iter(db.MultiValue(build_name, 'doi2users'), "recommendations.calculate_scores(doi2users)"):
        doi = id_struct.unpack(doi)[0]
        doi2users[doi] = array.array('I', (id_struct.unpack(user)[0] for user in users))

    for user, dois in util.notifying_iter(db.MultiValue(build_name, 'user2dois'), "recommendations.calculate_scores(user2dois)"):
        user = id_struct.unpack(user)[0]
        if len(dois) < settings.max_downloads_per_user:  # drop the ~0.1% of users that cause most of the work
            user2dois[user] = array.array('I', (id_struct.unpack(doi)[0] for doi in dois))
        else:
            user2dois[user] = array.array('I')

    user_ids = db.Ids(build_name, 'user')
    doi_ids = db.Ids(build_name, 'doi')

    for doi_a, users_a in util.notifying_iter(enumerate(doi2users), "recommendations.calculate_scores(calculate)", interval=1000):
        doi2users_common = collections.Counter()

        for user in users_a:
            for doi in user2dois[user]:
                doi2users_common[doi] += 1

        def scores_a():
            for doi_b, users_common in doi2users_common.iteritems():
                if doi_b != doi_a:
                    users_b = doi2users[doi_b]
                    score = (users_common ** 2.0) / len(users_a) / len(users_b)
                    yield (score, doi_b)

        top_scores = heapq.nlargest(limit, scores_a(), key=operator.itemgetter(0))
        scores.put(doi_ids.get_string(doi_a), [(score, doi_ids.get_string(doi_b)) for score, doi_b in top_scores])

def build(build_name, limit=5):
    (num_users, num_dois) = collate_downloads(build_name)
    calculate_scores(num_users, num_dois, build_name)

# for easy profiling
if __name__ == '__main__':
    (num_users, num_dois) = collate_downloads(build_name='test')
    print 'Nums', num_users, num_dois
    calculate_scores(num_users, num_dois, build_name='test')
