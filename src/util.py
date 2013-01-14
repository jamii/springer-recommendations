import sys

def log(name, event):
    print name, event
    sys.stdout.flush()

def logged(name, iter, interval=10000):
    i = 0
    log(name, 'starting')
    for value in iter:
        i += 1
        if i % interval == 0:
            log(name, i)
        yield value
    log(name, 'finished')
