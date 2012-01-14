# Installation (on Ubuntu 11.04)

    # install dependencies
    sudo apt-get install build-essential erlang erlang-dev python python-dev python-django python-numpy python-scipy python-simplejson python-pyparsing python-flask nginx libcmph-dev git-core

    # under large workloads disco runs out of file descriptors
    cat 'fs.file-max = 1000000' >> /etc/sysctl.conf
    sysctl -p
    cat '* soft nofile 800000' >> /etc/security/limits.conf
    cat '* hard nofile 800000' >> /etc/security/limits.conf
    cat 'root soft nofile 800000' >> /etc/security/limits.conf
    cat 'root hard nofile 800000' >> /etc/security/limits.conf
    sudo reboot now

    # build disco (with my changes)
    git clone git://github.com/jamii/disco.git
    git checkout deploy
    cd disco
    make
    sudo make install
    sudo make install-core
    sudo make install-discodb

    # setup disco
    ssh-keygen (default options)
    cat .ssh/id_rsa.pub >> .ssh/authorized_keys
    disco start
    in web config:
      change node name from 'localhost' to hostname
      change workers to 4

    # build and setup springer-analytics
    git clone git@github.com:scattered-thoughts/springer-analytics.git
    cd springer-analytics
    cp analytics.conf /etc/nginx/sites-enabled/
    wget http://trac.edgewall.org/export/10732/trunk/contrib/htpasswd.py
    python htpasswd.py -c -b /etc/nginx/htpasswd $username $password
    service nginx restart
    nohup python server.py > server_log &
