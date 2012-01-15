"""Parse download logs dumped from mongodb"""

import re
import datetime

import mr

download_pattern = re.compile("{ _id: ObjectId\('([^']*)'\), d: ([^,]*), doi: \"([^\"]*)\", i: \"([^\"]*)\", s: ([^,]*), ip: \"([^\"]*)\" }")

class ParseDownloads(mr.Job):
    # input from mongodb dump

    @staticmethod
    def map(line, params):
        match = downloads.download_pattern.match(line)
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

def parse(dump='dump:downloads'):
    downloads = ParseDownloads().run(input=[dump])
    mr.print_errors(downloads)
    return downloads
