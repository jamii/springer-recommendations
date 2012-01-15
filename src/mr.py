"""Various map/reduce utils"""

import os
import os.path
import sys
import itertools
import warnings

import disco.core
import disco.worker.classic.func

def default_partition(key, partitions, params):
    return hash(key) % partitions

class Job(disco.core.Job):
    required_modules = [(name, name+'.py') for name in ['util', 'mr', 'downloads', 'histograms']]

    map_reader = staticmethod(disco.worker.classic.func.chain_reader)

    partition = staticmethod(default_partition)
    partitions = 16

def is_error(key):
    with warnings.catch_warnings(): # catch UnicodeWarning - it crashes the disco worker
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
    sys.stdout.write("Finished job '%s'" % job.__class__.__name__)
    has_errors = False
    for key, value in disco.core.result_iterator(job.wait()):
        if is_error(key):
            if not has_errors:
                has_errors = True
                sys.stdout.write(' with the following errors:\n')
            print '    ', value
    if not has_errors:
        sys.stdout.write('\n')

def write_results(job, root, formatter):
    for key, value in disco.core.result_iterator(job.results()):
        if not is_error(key):
            filename = os.path.join(root, key)
            directory = os.path.dirname(filename)
            if not os.path.exists(directory):
                os.makedirs(directory)
            with open(filename, 'w') as file:
                file.write(formatter(value))
