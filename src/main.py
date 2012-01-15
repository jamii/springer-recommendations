def build_all(dump='dump:downloads', directory='./histograms'):
    downloads = downloads.parse(dump)
    histograms.build(downloads, directory)
    downloads.purge()
