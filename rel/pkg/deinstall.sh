#!/usr/bin/bash

case $2 in
    DEINSTALL)
	echo "Stopping Snarl service."
	svcadm disable network/snarl
	;;
    POST-DEINSTALL)
	echo "Removing Snarl service."
	svccfg delete network/snarl
	echo "Please beware that database and logfiles have not been"
	echo "deleted! Neither have the snarl user or gorup."
	echo "If you don't need them any more remove the directories:"
	echo " /var/log/snarl"
	echo " /var/db/snarl"
	;;
esac
