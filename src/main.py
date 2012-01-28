import downloads
import histograms
import recommendations

def build_all(dump='dump:downloads', build_name='test'):
    downloads_job = downloads.parse(dump)
    histograms.build(downloads_job.wait(), build_name)
    recommendations.build(downloads_job.wait(), build_name)
    downloads_job.purge()
