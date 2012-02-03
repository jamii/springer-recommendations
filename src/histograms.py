"""Histograms of downloads over time for a given DOI"""

import datetime
import json
import os.path

import disco.core
import disco.util

import mr

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

class FindDataRange(mr.Job):
    # input from FetchDownloads

    partitions = 1

    @staticmethod
    def map((id, download), params):
        yield download['date'], None

    @staticmethod
    def reduce(iter, params):
        date, _ = iter.next()
        min_date = date
        max_date = date
        for date, _ in iter:
            min_date = min(min_date, date)
            max_date = max(max_date, date)
        yield 'min_date', min_date
        yield 'max_date', max_date

class BuildHistograms(mr.Job):
    # input from FetchDownloads

    @staticmethod
    def map((id, download), params):
        doi = download['doi']
        date = download['date']
        yield doi, date

    sort = True
    @staticmethod
    def reduce(iter, params):
        for doi, dates in disco.util.kvgroup(iter):
            yield doi, Histogram(dates, params['min_date'], params['max_date'])

def build(downloads, build_name='test'):
    find_data_range = FindDataRange().run(input=[downloads])
    data_range = dict(disco.core.result_iterator(find_data_range.wait()))
    find_data_range.purge()

    histograms = BuildHistograms().run(input=[downloads], params=data_range)

    def year_month(date):
        return datetime.date(date.year, date.month, 1)
    mr.write_results(histograms.wait(), build_name, 'histograms/monthly', lambda histogram: histogram.grouped_by(year_month).dumps())

    today = datetime.date.today()
    thirty_days_ago = today - datetime.timedelta(days=30)
    mr.write_results(histograms.wait(), build_name, 'histograms/daily', lambda histogram: histogram.restricted_to(thirty_days_ago, today).dumps())

    histograms.purge()
