# Installation (on Ubuntu 11.04)

Install dependencies

     sudo apt-get install build-essential erlang erlang-dev python python-dev ipython python-pymongo nginx git-core

Raise limit on number of file descriptors (nder large workloads disco runs out)

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
    sudo mkdir -p /mnt/var/mongodb
    sudo chown mongodb:mongodb /mnt/var/mongodb
    sudo mkdir -p /mnt/var/log/mongodb/
    sudo chown mongodb:mongodb /mnt/var/mongodb
    sudo service mongodb restart

(nginx setup will be here)
(cron setup will be here)

# Operation

The recommender uses dumps of the mongodb log database, stored in the disco distributed filesystem.

To add a log file to ddfs:

    sudo ddfs chunk live:downloads ./log_file

To remove all log files from ddfs:

   sudo ddfs rm live:downloads

To build the download histograms and recommendations:

   cd springer-recommendations/src
   nohup python -c 'import main; main.build_all(dump="live:downloads")'

You can watch the progress in the disco web config.
