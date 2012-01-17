import downloads
import histograms
import recommendations

def build_all(dump='dump:downloads', histograms_dir='./histograms', recommendations_dir='./recommendations'):
    downloads_job = downloads.parse(dump)
    histograms.build(downloads_job.wait(), histograms_dir)
    recommendations.build(downloads_job.wait(), recommendations_dir)
    downloads_job.purge()
