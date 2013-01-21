Generates 'people who read this also read...'-style recommendations based on the [Jaccard similarity](http://en.wikipedia.org/wiki/Jaccard_index) between their readership sets. This code scales to large (1B rows) datasets in limited memory by using external sorting and [locality sensitive hashing](http://en.wikipedia.org/wiki/Locality_sensitive_hashing).

# Installation (on Ubuntu 11.04)

Install dependencies

``` bash
sudo apt-get install build-essential python python-dev python-pip ipython git-core
# bson must be installed before pymongo since the pymongo package both depends on bson and overwrites it :(
sudo pip install bson
sudo pip install pymongo
sudo pip install ujson
```

Setup springer-recommendations

``` bash
git clone git://github.com/jamii/springer-recommendations.git
sudo mkdir -p /mnt/var/springer-recommendations
sudo chown $USER:$USER /mnt/var/springer-recommendations
```

# Operation

The recommendations engine reads in a list of filenames pointing to dumps from the mongodb log database and prints out a list of recommendations.

Example usage:

``` bash
find /mnt/var/Mongo3-backup/*.bson | python springer-recommendations/src/recommendations.py > recommendations.json
```

For each DOI in the logs, the recommendations contain a row of recommended DOIs and their Jaccard similarity to the first DOI.

``` json
["10.1007/s00216-012-5950-6",
  [
    ["10.1007/BF01209646", 0.09091],
    ["10.1007/s00216-011-5567-1", 0.09091],
    ["10.1007/s00216-011-5576-0", 0.09091],
    ["10.1007/s00216-012-5732-1", 0.09091],
    ["10.1007/s00216-012-6027-2", 0.09091]
  ]
]
```
