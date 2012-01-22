"""Various map/reduce utils"""

import os
import os.path
import sys
import itertools
import warnings

import disco.core
import disco.util
import disco.worker.classic.func

import settings

def default_partition(key, partitions, params):
    return hash(key) % partitions

class Job(disco.core.Job):
    required_modules = [(name, name+'.py') for name in ['settings', 'util', 'cache', 'mr', 'db', 'downloads', 'histograms', 'recommendations']]

    map_reader = staticmethod(disco.worker.classic.func.chain_reader)

    partition = staticmethod(default_partition)
    partitions = settings.default_number_of_partitions

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

def write_results(input, root, formatter):
    for key, value in disco.core.result_iterator(input):
        if not is_error(key):
            filename = os.path.join(settings.root_directory, root, key)
            directory = os.path.dirname(filename)
            if not os.path.exists(directory):
                os.makedirs(directory)
            with open(filename, 'w') as file:
                file.write(formatter(value))

def group(iter, params):
    return disco.util.kvgroup(iter)

def group_uniq(iter, params):
    for key, values in disco.util.kvgroup(iter):
        yield key, list(set(values))

# given urls for two sets of inputs, zip the urls for zip_reader
def zip(input_a, input_b):
    return '\n'.join([input_a, input_b])

def zip_reader(stream, size, url):
    [url_a, url_b] = url.split('\n')
    reader_a = disco.worker.classic.func.chain_reader(input_a)
    reader_b = disco.worker.classic.func.chain_reader(input_b)
    for ((k_a, v_a), (k_b, v_b)) in itertools.izip(reader_a, reader_b):
        assert (k_a == k_b)
        yield (k_a, (v_a, v_b))
