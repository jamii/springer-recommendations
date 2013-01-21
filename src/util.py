import sys
from datetime import datetime
import functools

def log(name, event):
    sys.stderr.write("%s %s - %s\n" % (datetime.now(), name, event))
    sys.stderr.flush()

def timed(fn):
    @functools.wraps(fn)
    def wrapped(*args):
        log(fn.func_name, 'started')
        result = fn(*args)
        log(fn.func_name, 'finished')
        return result
    return wrapped
