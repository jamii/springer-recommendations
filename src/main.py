import datetime

import downloads
import histograms
import recommendations

def build_all(db_name, collection_name, start_date=datetime.date.min, build_name='test'):
    downloads.fetch(db_name, collection_name, start_date)
    # histograms.build(build_name)
    recommendations.build(build_name)
