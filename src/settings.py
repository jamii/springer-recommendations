root_directory = '/mnt/var/springer-recommendations'
db_cache_size = 10000
default_number_of_partitions = 4 # for best performance this should be the same as the total number of disco workers (check the disco web config)
max_downloads_per_user = 1000 # users with more downloads will be ignored, as they cause the majority of the work and probably don't represent a real person
