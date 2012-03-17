"""
Quick tool to preview recommendations
"""

import httplib, urllib
import json

import util
import db

keys = json.load(open('keys'))

def metadata(doi):
    conn = httplib.HTTPConnection('springer.api.mashery.com')
    path = '/metadata/json?%s' % urllib.urlencode({'q':'doi:'+doi, 'api_key':keys['metadata']})
    conn.request('GET', path)
    response = conn.getresponse()
    status = response.status
    data = response.read()
    conn.close()

    if status == 200:
        meta = json.loads(data)
        if meta['records']:
            return meta
        else:
            # sometimes get an empty response rather than a 404
            raise db.NotFound('fetch:empty', doi)
    elif status == 404:
        raise db.NotFound('fetch:404', doi)
    else:
        raise CommError(data, 'http://api.springer.com' + path, code=status)

def title(doi):
    return metadata(doi)['records'][0]['title']

def link(doi):
    return "http://www.springerlink.com/index/" + doi

def recommendations(build_name, doi):
    print title(doi)
    print link(doi)
    print
    scores = db.SingleValue(build_name, 'scores', 'r').get(doi)
    print '-' * 40
    for (score, doi) in scores:
        print score
        print title(doi)
        print link(doi)
        print '-' * 40
