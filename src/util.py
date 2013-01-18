import sys
from datetime import datetime
import functools

def log(name, event):
    print datetime.now(), name, '-', event
    sys.stdout.flush()

class Timed:
    def __init__(self, name):
        self.name = name

    def __enter__(self):
        log(self.name, 'starting')

    def __exit__(self, type, value, traceback):
        log(self.name, 'finished')

def timed(fn):
    @functools.wraps(fn)
    def wrapped(*args):
        log(fn.func_name, 'started')
        result = fn(*args)
        log(fn.func_name, 'finished')
        return result
    return wrapped
