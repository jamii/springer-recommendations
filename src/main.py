import datetime

import downloads
import histograms
import recommendations

def build_all(db_name, collection_name, start_date=datetime.date.min, build_name='test'):
    downloads_iter = downloads.fetch(db_name, collection_name, start_date)
    histograms.build(downloads_job.wait(), build_name)
    recommendations.build(downloads_iter, build_name)
