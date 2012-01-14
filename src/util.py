import disco.core
from disco.worker.classic.func import chain_reader
from disco.util import kvgroup

from socket import gethostname
from gzip import GzipFile
import os.path
import datetime
import itertools
import warnings
import re
import json

def default_partition(key, partitions, params):
    return hash(key) % partitions

class Job(disco.core.Job):
    required_modules = [('util', 'util.py'), ('db', 'db.py'), ('metadata', 'metadata.py'), ('jobs', 'jobs.py'), ('query', 'query.py'), ('data', 'data.py')]

    map_reader = staticmethod(chain_reader)

    partition = staticmethod(default_partition)
    partitions = 16

def is_error(key):
    with warnings.catch_warnings(): # UnicodeWarning
        warnings.simplefilter("ignore")
        return key in [u'error', 'error']

def map_with_errors(map):
    def new_map((key, value), params):
        if not is_error(key):
            return map((key, value), params)
        else:
            return iter([])
    return new_map

def reduce_with_errors(reduce):
    def new_reduce(iter, params):
        iter = itertools.ifilter(lambda (key, value): not is_error(key), iter)
        return reduce(iter, params)
    return new_reduce

def print_errors(job):
    print job.__class__.__name__
    for key, value in disco.core.result_iterator(job.wait()):
        if is_error(key):
            print '\t', value

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
    logs = open(log_file)
    out = open(out_file, 'w')
    pattern = re.compile('doi: "([^"]*)"')
    for log in logs:
        match = pattern.search(log)
        if match and match.group(1) in dois:
            out.write(log)
