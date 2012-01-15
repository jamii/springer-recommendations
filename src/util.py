import gzip
import datetime
import re
import itertools

def encode(js):
    """Convert all unicode objects in a json structure to str<utf8> for disco interop"""
    if type(js) is dict:
        return dict([(encode(key), encode(value)) for key, value in js.items()])
    elif type(js) is list:
        return [encode(elem) for elem in js]
    elif type(js) is unicode:
        return js.encode('utf8')
    else:
        return js

def date(string):
    """Date of a yyyy-mm-dd string"""
    return datetime.date(int(string[0:4]), int(string[5:7]), int(string[8:10]))

def date_range(start, end):
    day = datetime.timedelta(days=1)
    while start <= end:
        yield start
        start += day

def flatten(listOfLists):
    return itertools.chain.from_iterable(listOfLists)

def filter_logs(doi_file, log_file, out_file):
    dois = set(open(doi_file).read().split())
    logs = gzip.open(log_file)
    out = open(out_file, 'w')
    pattern = re.compile('doi: "([^"]*)"')
    for log in logs:
        match = pattern.search(log)
        if match and match.group(1) in dois:
            out.write(log)
