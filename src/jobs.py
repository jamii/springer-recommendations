import re
import datetime
import random
import os.path

from disco.util import kvgroup
from disco.error import CommError
from disco.core import result_iterator

from util import Job, map_with_errors, reduce_with_errors, print_errors
import data

download_pattern = re.compile("{ _id: ObjectId\('([^']*)'\), d: ([^,]*), doi: \"([^\"]*)\", i: \"([^\"]*)\", s: ([^,]*), ip: \"([^\"]*)\" }")

class ParseDownloads(Job):
    @staticmethod
    def map(line, params):
        match = jobs.download_pattern.match(line)
        if match:
            (id, date, doi, _, _, ip) = match.groups()
            download = {
                'id':id.decode('latin1').encode('utf8'),
                'doi':doi.decode('latin1').encode('utf8'),
                'date':datetime.date(int(date[0:4]), int(date[4:6]), int(date[6:8])),
                'ip':ip.decode('latin1').encode('utf8')
                }
            yield id, download
        else:
            yield 'error', line

class FindDataRange(Job):
    partitions = 1

    @staticmethod
    @map_with_errors
    def map((id, download), params):
        yield download['date'], None

    @staticmethod
    @reduce_with_errors
    def reduce(iter, params):
        date, _ = iter.next()
        min_date = date
        max_date = date
        for date, _ in iter:
            min_date = min(min_date, date)
            max_date = max(max_date, date)
        yield 'min_date', min_date
        yield 'max_date', max_date

class BuildHistograms(Job):
    sort = True

    @staticmethod
    @map_with_errors
    def map((id, download), params):
        doi = download['doi']
        date = download['date']
        yield doi, date

    @staticmethod
    @reduce_with_errors
    def reduce(iter, params):
        for doi, dates in kvgroup(iter):
            yield doi, data.Histogram(dates, params['min_date'], params['max_date'])

def write_results(job, root, formatter):
    for key, value in result_iterator(job.results()):
        filename = os.path.join(root, key)
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(filename, 'w') as file:
            file.write(formatter(value))

def parse_downloads(dump='dump:downloads'):
    downloads = ParseDownloads().run(input=[dump])
    print_errors(downloads)
    return downloads

def build_histograms(downloads, directory='./histograms'):
    find_data_range = FindDataRange().run(input=[downloads.wait()])
    print_errors(find_data_range)
    data_range = dict(result_iterator(find_data_range.results()))
    find_data_range.purge()

    histograms = BuildHistograms().run(input=[downloads.wait()], params=data_range)
    print_errors(histograms)

    def year_month(date):
        return datetime.date(date.year, date.month, 1)
    write_results(histograms, os.path.join(directory, 'monthly'), lambda histogram: histogram.grouped_by(year_month).dumps())

    today = datetime.date.today()
    thirty_days_ago = today - datetime.timedelta(days=30)
    write_results(histograms, os.path.join(directory, 'daily'), lambda histogram: histogram.restricted_to(thirty_days_ago, today).dumps())

    histograms.purge()

def build_all(dump='dump:downloads'):
    downloads = parse_downloads(dump)
    build_histograms(downloads)
    downloads.purge()
