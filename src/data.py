import datetime
import json

import util

class Histogram():
    def __init__(self, items, start_date, end_date):
        # start_date and end_date are inclusive
        self.start_date = start_date
        self.end_date = end_date
        self.counts = {}
        for item in items:
            self.counts[item] = self.counts.get(item, 0) + 1

    def __str__(self):
        return repr(self)

    def __repr__(self):
        items = [date for date, count in self.counts.items() for i in xrange(0, count)]
        return "Histogram(%s, %s, %s)" % (repr(items), repr(self.start_date), repr(self.end_date))

    def __contains__(self, item):
        return (self.start_date <= item <= self.end_date)

    def __getitem__(self, item):
        if item in self.counts:
            return self.counts[item]
        elif self.start_date <= item <= self.end_date:
            return 0
        else:
            raise KeyError(item)

    def __iter__(self):
        return util.date_range(self.start_date, self.end_date)

    def grouped_by(self, grouper):
        # grouper must be monotonic and must return a date
        counts = {}
        for date, count in self.counts.items():
            counts[grouper(date)] = counts.get(date, 0) + count
        histogram = Histogram([], self.start_date, self.end_date)
        histogram.counts = counts
        return histogram

    def restricted_to(self, start_date, end_date):
        histogram = Histogram([], max(start_date, self.start_date), min(end_date, self.end_date))
        histogram.counts = dict(((date,value) for date,value in self.counts.items() if start_date <= date <= end_date))
        return histogram

    def total(self):
        return sum(self.counts.values())

    def dumps(self):
        counts = sorted( [(str(date), value) for date, value in self.counts.items()] )
        return json.dumps({'start_date': str(self.start_date), 'end_date': str(self.end_date), 'counts': counts})
