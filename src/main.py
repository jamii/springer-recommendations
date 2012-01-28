import downloads
import histograms
import recommendations

def build_all(input=['test:downloads'], build_name='test'):
    downloads_job = downloads.parse(input)
    histograms.build(downloads_job.wait(), build_name)
    recommendations.build(downloads_job.wait(), build_name)
    downloads_job.purge()
