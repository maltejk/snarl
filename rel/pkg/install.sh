#!/usr/bin/bash

USER=snarl
GROUP=$USER
DBID=4
BACKUP_FILE=/var/db/snarl/backup_$DBID.tar.gz

case $2 in
    PRE-INSTALL)
        #if grep '^Image: base64 1[43].[1234].*$' /etc/product
        #then
        #    echo "Image version supported"
        #else
        #    echo "This image version is not supported please use the base64 13.2.1 image or later."
        #    exit 1
        #fi
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
            useradd -g $GROUP -d /var/db/snarl -s /bin/false $USER
        fi
        echo Creating directories ...
        mkdir -p /var/db/snarl/ring
        mkdir -p /var/db/snarl/users
        mkdir -p /var/db/snarl/groups
        chown -R snarl:snarl /var/db/snarl
        mkdir -p /var/log/snarl/sasl
        chown -R snarl:snarl /var/log/snarl
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
        ;;
    POST-INSTALL)
        echo Importing service ...
        svccfg import /opt/local/fifo-snarl/share/snarl.xml
        echo Trying to guess configuration ...
        IP=`ifconfig net0 | grep inet | awk -e '{print $2}'`
        CONFFILE=/opt/local/fifo-snarl/etc/snarl.conf
        if [ ! -f "${CONFFILE}" ]
        then
            cp ${CONFFILE}.example ${CONFFILE}
            sed --in-place -e "s/127.0.0.1/${IP}/g" ${CONFFILE}
        else
            /opt/local/fifo-snarl/share/update_config.sh ${CONFFILE}.example ${CONFFILE} > ${CONFFILE}.new &&
                mv ${CONFFILE} ${CONFFILE}.old &&
                mv ${CONFFILE}.new ${CONFFILE}
        fi
        ;;
esac
