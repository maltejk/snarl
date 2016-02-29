#!/usr/bin/bash

AWK=/usr/bin/awk
SED=/usr/bin/sed

USER=snarl
GROUP=$USER
DBID=5
BACKUP_FILE=/var/db/snarl/backup_$DBID.tar.gz

case $2 in
    PRE-INSTALL)
        if grep "^$GROUP:" /etc/group > /dev/null 2>&1
        then
            echo "Group already exists, skipping creation."
        else
            echo Creating snarl group ...
            groupadd $GROUP
        fi
        if id $USER > /dev/null 2>&1
        then
            echo "User already exists, skipping creation."
        else
            echo Creating snarl user ...
            useradd -g $GROUP -d /data/snarl -s /bin/false $USER
        fi
        echo Creating directories ...
        mkdir -p /data/snarl/db/ring
        mkdir -p /data/snarl/etc
        mkdir -p /data/snarl/log/sasl
        chown -R $USER:$GROUP /data/snarl

        if [ -d /var/db/snarl/0 ]
        then
            if [ ! -f $BACKUP_FILE ]
            then
                echo "############################################################"
                echo "# This release introduces a update in the Database!        #"
                echo "# To ensure a a fallback option a backup file if the       #"
                echo "# database is created                                      #"
                echo "############################################################"
                echo $BACKUP_FILE
                tar cfz $BACKUP_FILE /var/db/snarl/{[0-9]*,ring}
            fi
        fi
        if [ -d /tmp/snarl ]
        then
            chown -R $USER:$GROUP /tmp/snarl/
        fi

        ;;
    POST-INSTALL)
        echo Importing service ...
        svccfg import /opt/local/fifo-snarl/share/snarl.xml
        echo Trying to guess configuration ...
        IP=`ifconfig net0 | grep inet | $AWK '{print $2}'`

        CONFFILE=/data/snarl/etc/snarl.conf
        cp /opt/local/fifo-snarl/etc/snarl.conf.example ${CONFFILE}.example

        if [ ! -f "${CONFFILE}" ]
        then
            echo "Creating new configuration from example file."
            cp ${CONFFILE}.example ${CONFFILE}
            $SED -i bak -e "s/127.0.0.1/${IP}/g" ${CONFFILE}
        else
            echo "Please make sure you update your config according to the update manual!"
            #/opt/local/fifo-sniffle/share/update_config.sh ${CONFFILE}.example ${CONFFILE} > ${CONFFILE}.new &&
            #    mv ${CONFFILE} ${CONFFILE}.old &&
            #    mv ${CONFFILE}.new ${CONFFILE}
        fi
        ;;
esac
