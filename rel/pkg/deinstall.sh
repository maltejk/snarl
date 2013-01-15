#!/usr/bin/bash

case $2 in
    DEINSTALL)
	;;
    POST-DEINSTALL)
	echo "Please beware that database and logfiles have not been"
	echo "deleted! Neither have the snarl service, user or gorup."
	echo "If you don't need them any more remove the directories:"
	echo " /var/log/snarl"
	echo " /var/db/snarl"
    rm /opt/local/sbin/snaadm
	;;
esac
