# Installation (on Ubuntu 11.04)

Install dependencies

     sudo apt-get install build-essential erlang erlang-dev python python-dev ipython python-pymongo nginx git-core

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

 Setup springer-recommendations

    git clone git://github.com/jamii/springer-recommendations.git
    sudo mkdir -p /mnt/var/springer-recommendations
    sudo chown $USER:$USER /mnt/var/springer-recommendations

 Setup mongodb

    sudo cp mongodb.conf /etc/mongodb.conf
    sudo mkdir -p /mnt/var/lib/mongodb
    sudo chown mongodb:mongodb /mnt/var/lib/mongodb
    sudo mkdir -p /mnt/var/log/mongodb/
    sudo chown mongodb:mongodb /mnt/var/log/mongodb
    sudo service mongodb restart

(nginx setup will be here)
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

Regression tests are used to catch changes in output between different versions of the code or different system configurations eg

    sudo ddfs chunk test:downloads ./some_small_log_file
    cd springer-recommendations/src
    git checkout master
    python -c 'import main; main.build_all(input=["test:downloads"], build_name="regression-master")'
    git checkout some_branch
    python -c 'import main; main.build_all(input=["test:downloads"], build_name="regression-branch")'
    python -c 'import test; test.regression("regression-master", "regression-branch")

The regression test walks both the directory trees for each build in parallel at stop at the first difference encountered eg:

    Filenames do not match:
    /mnt/var/springer-recommendations/test1/histograms/daily/10.1007/BF00379779
    /mnt/var/springer-recommendations/test2/histograms/daily/10.1007/BF02247133
    Traceback (most recent call last):
      File "<string>", line 1, in <module>
      File "test.py", line 42, in regression
        raise RegressionError()
    test.RegressionError

Passing tests will look like this:

    Regression test passed for builds:
    regression-master
    regression-branch
    (1788 files compared)
