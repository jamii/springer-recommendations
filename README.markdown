# Installation (on Ubuntu 11.04)

Install dependencies

     sudo apt-get install build-essential erlang erlang-dev python python-dev ipython python-pymongo nginx git-core mongodb autoconf automake libtool pkg-config

Raise limit on number of file descriptors (under large workloads disco runs out)

    sudo bash -c "echo 'fs.file-max = 1000000' >> /etc/sysctl.conf"
    sysctl -p
    sudo bash -c "echo '
        * soft nofile 800000
        * hard nofile 800000
        root soft nofile 800000
        root hard nofile 800000
        ' >> /etc/security/limits.conf"
    sudo reboot now

Build disco (with my changes)

    git clone git://github.com/jamii/disco.git
    cd disco
    git checkout -b deploy origin/deploy
    make
    sudo make install
    sudo make install-core

Setup disco

    ssh-keygen # choose default options
    cat .ssh/id_rsa.pub >> .ssh/authorized_keys
    sudo disco start
    # in disco web config (localhost:8989) click configure and:
      # change node name from 'localhost' to hostname
      # change number of workers to 2 * number of cores
    # click status and you should now see that the background of the hostname has changed from red to black

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

Setup nginx

    sudo mkdir /usr/local/nginx # for some reason the ubuntu package does not create this
    sudo cp nginx.conf /etc/nginx/sites-available/springer-recommendations.conf
    sudo ln -s /etc/nginx/sites-available/springer-recommendations.conf /etc/nginx/sites-enabled/springer-recommendations.conf
    sudo rm /etc/nginx/sites-enabled/default
    sudo service nginx restart

(cron setup will be here)

# Operation

The recommender uses dumps of the mongodb log database, stored in the disco distributed filesystem. DDFS is a tag based filesystem - each file can have multiple tags and each tag can refer to multiple files. Tags consist of ':' separated alphanumeric strings eg 'live:downloads', 'test:regression:downloads'

To add a log file to ddfs under the tag 'live:downloads':

    sudo ddfs chunk live:downloads ./log_file

To remove all log files under the tag 'live:downloads':

    sudo ddfs rm live:downloads

To build the download histograms and recommendations using all files under the tag 'live:downloads':

    cd springer-recommendations/src
    nohup python -c 'import main; main.build_all(input=["live:downloads"], build_name="live")'
    # nginx link will be here

You can watch the progress in the disco web config.

Results are written to /mnt/var/springer-recommendations. The build name determines the directory the results will be stored in and the mongodb database that will be used for intermediate results. This makes it easy to run tests without messing with production data.

# Testing

Unit tests compare results for a simple dataset to hand-solved results:

    springer-recommendations/src
    python -c 'import test; test.unit_base()'

Regression tests are used to catch changes in output between different versions of the code or different system configurations eg

    sudo ddfs chunk test:downloads ./some_small_log_file
    cd springer-recommendations/src
    git checkout master
    python -c 'import main; main.build_all(input=["test:downloads"], build_name="regression-master")'
    git checkout some_branch
    python -c 'import main; main.build_all(input=["test:downloads"], build_name="regression-branch")'
    python -c 'import test; test.regression("regression-master", "regression-branch")

The regression test walks the directory trees for each build in parallel and stops at the first difference encountered eg:

    Filenames do not match:
    /mnt/var/springer-recommendations/test1/histograms/daily/10.1007/BF00379779
    /mnt/var/springer-recommendations/test2/histograms/daily/10.1007/BF02247133
    Traceback (most recent call last):
      File "<string>", line 1, in <module>
      File "test.py", line 42, in regression
        raise RegressionError()
    test.RegressionError

# API

All results are written to the file-system under their build name and served by nginx. For example, if you wanted results from the 'live' build:

    ubuntu@domU-12-31-39-16-CC-12:~$ curl localhost:80/springer-recommendations/live/histograms/monthly/10.1007/s10853-009-4131-2
    {"counts": [["2011-01-01", 1], ["2011-02-01", 1], ["2011-03-01", 1], ["2011-04-01", 2], ["2011-05-01", 3]], "start_date": "2011-01-07", "end_date": "2011-05-19"}

    ubuntu@domU-12-31-39-16-CC-12:~$ curl localhost:80/springer-recommendations/live/histograms/daily/10.1007/s10853-009-4131-2
    {"counts": [], "start_date": "2011-12-30", "end_date": "2011-05-19"}

    ubuntu@domU-12-31-39-16-CC-12:~$ curl localhost:80/springer-recommendations/live/recommendations/10.1007/s10853-009-4131-2
    [[0.1258741258741259, "10.1007/s10853-010-4213-1"], [0.11538461538461539, "10.1023/B:JMSC.0000048768.52085.63"], [0.11538461538461539, "10.1023/B:JMSC.0000048767.92292.df"], [0.11538461538461539, "10.1023/B:JMSC.0000047544.44078.ca"], [0.11538461538461539, "10.1023/B:JMSC.0000045664.59279.e4"]]
