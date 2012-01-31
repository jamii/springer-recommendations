"""Parse download logs dumped from mongodb"""

import re
import datetime

import mr
import pymongo

import disco.schemes.scheme_raw

# for some reason the dates are stored as integers...
def date_to_int(date):
    return date.year * 10000 + date.month * 100 + date.day
def int_to_date(int):
    return datetime.date(int // 10000, int % 10000 // 100, int % 100)

class FetchDownloads(mr.Job):
    # fetch downloads from mongodb

    map_reader = staticmethod(disco.schemes.scheme_raw.input_stream)

    @staticmethod
    def map(line, params):
        collection = pymongo.Connection()[params['db_name']][params['collection_name']]
        d = date_to_int(params['start_date'])
        logs = collection.find({'d':{'$gte':d}})
        for log in logs:
            id = str(log['_id'])
            doi = log['doi'].encode('utf8')
            date = int_to_date(int(log['d']))
            ip = log['ip'].encode('utf8')
            yield id, {'id':id, 'doi':doi, 'date':date, 'ip':ip}

def fetch(db_name, collection_name, start_date=datetime.date.min):
    params = {'db_name':db_name, 'collection_name':collection_name, 'start_date':start_date}
    downloads = FetchDownloads().run(input=['raw://foo'], params=params)
    return downloads
