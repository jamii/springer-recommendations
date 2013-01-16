import sys
from datetime import datetime
import functools

def log(name, event):
    print datetime.now(), name, event
    sys.stdout.flush()

def logged(fn):
    @functools.wraps(fn)
    def wrapped(*args):
        log(fn.func_name, 'started')
        result = fn(*args)
        log(fn.func_name, 'finished')
        return result
    return wrapped
