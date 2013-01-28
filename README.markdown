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

The recommendations engine reads a newline-separated list of input filenames on stdin and prints a list of recommendations on stdout.

Each input file should contain a newline-separated list of json-encoded [user, doi] pairs. The user field may be any unique string eg ip address or session id.

``` json
["1yud2mlgpalm2cqeyyz0o44n","10.1007\/s10526-004-6592-1"]
["q4lprrkmbr3gpvosjao0dzwm","10.1007\/978-3-540-69934-7_13"]
["3jc2hnohgreyhvlurpg3m1sn","10.1007\/978-3-8348-8229-5_14"]
["uigkldnerjvgghxvjp2ptm0i","10.1007\/s00125-009-1355-2"]
["mmnqkjwawkcz4tqjcxfam4jz","10.1007\/978-3-8274-2313-9_4"]
["e3ie31mmad2epuxno1gpidmx","10.1007\/s10549-012-2108-3"]
["1adokad3mbbg0aaexcl1yb3a","10.1007\/978-3-8349-6622-3_5"]
["fzfrjqgnizgprfxstcal12fu","10.1007\/978-3-7643-8777-8_1"]
["ihcnriijo040rchrgbytvlpg","10.1007\/BF00309663"]
```

For each DOI in the logs, the output contains a line of related DOIs and their Jaccard similarity to the first DOI.

``` json
["10. 1007\/s11547-008-0266-5",[[0.07143,"10.1007\/978-88-470-0567-9_5"],[0.07143,"10.1007\/BF02358413"],[0.07143,"10.1007\/BF02747734"],[0.125,"10.1007\/s11547-009-0471-x"],[0.07692,"10.1007\/s12009-000-0010-9"]]]
["10.+1007\/s11547-008-0266-5",[[0.02632,"10.1007\/s11547-006-0099-z"],[0.01923,"10.1007\/s11547-007-0206-9"],[0.0119,"10.1007\/s11547-008-0279-0"]]]
["10.0007\/s001640000002",[[0.30682,"10.0007\/s001640000003"],[0.03774,"10.1007\/978-0-387-35851-2_24"],[0.06,"10.1007\/978-0-387-35851-2_4"],[0.10526,"10.1007\/s10443-008-9061-7"],[0.0381,"10.1007\/s11630-010-0389-6"]]]
["10.0007\/s001640000003",[[0.30682,"10.0007\/s001640000002"],[0.05333,"10.1007\/978-0-387-35851-2_24"],[0.07143,"10.1007\/978-0-387-35851-2_4"],[0.06952,"10.1007\/s10443-008-9061-7"],[0.06452,"10.1134\/1.1574363"]]]
["10.0007\/s001640000004",[[0.11765,"10.1007\/s00164-001-0012-z"],[0.11111,"10.1007\/s00164-001-0017-7"],[0.14286,"10.1007\/s00164-001-0024-8"],[0.13333,"10:1007\/s00164-001-0017-7"],[0.14286,"10:1007\/s00164-001-0020-z"]]]
```

Example usage:

``` bash
find /mnt/var/springer-recommendations/logs-*.json | nohup python springer-recommendations/src/recommendations.py > recommendations.json 2> recommendations.log &
```
