import sys
from datetime import datetime
import functools

def log(name, event):
    print datetime.now(), name, event
    sys.stdout.flush()

def log_iter(name, iter, interval=1000000):
    i = 0
    log(name, 'starting')
    for value in iter:
        i += 1
        if i % interval == 0:
            log(name, i)
        yield value
    log(name, 'finished')

def logged(fn):
    @functools.wraps(fn)
    def wrapped(*args):
        log(fn.func_name, 'started')
        result = fn(*args)
        log(fn.func_name, 'finished')
        return result
    return wrapped

def logged_gen(fn, interval=1000000):
    @functools.wraps(fn)
    def wrapped(*args):
        return log_iter(fn.func_name, fn(*args), interval=interval)
    return logged(wrapped)
