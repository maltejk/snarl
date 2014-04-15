#!/usr/bin/bash

USER=snarl
GROUP=$USER
DBID=2
BACKUP_FILE=/var/db/snarl/backup_$DBID.tar.gz

case $2 in
    PRE-INSTALL)
        if grep '^Image: base64 13.[234].*$' /etc/product
        then
            echo "Image version supported"
        else
            echo "This image version is not supported please use the base64 13.2.1 image."
            exit 1
        fi
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
            md5sum ${CONFFILE} > ${CONFFILE}.md5
        elif [ -f ${CONFFILE}.md5 ]
        then
            if md5sum --quiet --strict -c ${CONFFILE}.md5 2&> /dev/null
            then
                echo "The config was not adjusted we'll regenerate it."
                cp ${CONFFILE}.example ${CONFFILE}
                sed --in-place -e "s/127.0.0.1/${IP}/g" ${CONFFILE}
                md5sum ${CONFFILE} > ${CONFFILE}.md5
            fi
        else
            mv ${CONFFILE} ${CONFFILE}.old
            cat ${CONFFILE}.old | grep -v mdns.server | grep -v anti_entropy.max_open_files | grep -v db.dir | grep -v anti_entropy.write_buffer_size | grep -v platform_data_dir > ${CONFFILE}
            echo anti_entropy.write_buffer_size_min = 4MB >> ${CONFFILE}
            echo anti_entropy.write_buffer_size_max = 4MB >> ${CONFFILE}
        fi
        cp /opt/local/fifo-snarl/bin/snaadm /opt/local/sbin
        ;;
esac
