"""Parse download logs dumped from mongodb"""

import re
import datetime
import pymongo

import util
import db

# for some reason the dates are stored as integers...
def date_to_int(date):
    return date.year * 10000 + date.month * 100 + date.day
def int_to_date(int):
    return datetime.date(int // 10000, int % 10000 // 100, int % 100)

def fetch(db_name, collection_name, build_name, start_date=datetime.date.min):
    downloads = db.SingleValue(build_name, 'downloads')
    collection = pymongo.Connection()[db_name][collection_name]

    d = date_to_int(start_date)
    logs = collection.find({'d':{'$gte':d}, 'si':{'$exists':true}})
    for log in util.notifying_iter(logs, "downloads.fetch", interval=10000):
        id = str(log['_id'])
        doi = log['doi'].encode('utf8')
        date = int_to_date(int(log['d']))
        ip = log['ip'].encode('utf8')
        si = log['si'].encode('utf8')
        downloads.put(id, {'id':id, 'doi':doi, 'date':date, 'ip':ip, 'si':si})
