# Installation (on Ubuntu 11.04)

Install dependencies

     sudo apt-get install build-essential python python-dev ipython python-pymongo git-core mongodb autoconf automake libtool pkg-config

Setup py-leveldb

    svn checkout http://py-leveldb.googlecode.com/svn/trunk/ py-leveldb-read-only
    cd py-leveldb-read-only
    ./compile_leveldb.sh
    python setup.py build
    sudo python setup.py install

Setup springer-recommendations

    git clone git://github.com/jamii/springer-recommendations.git
    sudo mkdir -p /mnt/var/springer-recommendations
    sudo chown $USER:$USER /mnt/var/springer-recommendations

Setup mongodb

    sudo service mongodb stop
    sudo cp springer-recommendations/mongodb.conf /etc/mongodb.conf
    sudo mkdir -p /mnt/var/lib/mongodb
    sudo chown mongodb:mongodb /mnt/var/lib/mongodb
    sudo mkdir -p /mnt/var/log/mongodb/
    sudo chown mongodb:mongodb /mnt/var/log/mongodb
    sudo service mongodb start

(cron setup will be here)

# Operation

The recommender pulls log records from mongodb and stores the results in leveldb.

To run the recommender:

    ./build --db_name=Mongo3-backup --collection_name=LogsRaw --build_name=live

The build_name determines where intermediate data and results are stored. Successive calls to build with the same build_name will reuse the intermediate data where possible. To help reuse, specify a start_date. All logs which were inserted before the start_date will be assumed to have been handled in a previous build.

    ./build --db_name=Mongo3-backup --collection_name=LogsRaw --build_name=live --start_date=2012-02-18

Results are written to /mnt/var/springer-recommendations/$build_name.

# Testing

Unit tests compare results for a simple dataset to hand-solved results:

    ./test-unit

Regression tests are used to catch changes in output between different versions of the code or different system configurations eg

    cd springer-recommendations
    git checkout master
    ./build --db_name=Mongo3-backup --collection_name=LogsRaw --build_name=test-master
    git checkout some-branch
    ./build --db_name=Mongo3-backup --collection_name=LogsRaw --build_name=test-some-branch
    ./test-regression --old_build=test-master --new_build=test-some-branch
