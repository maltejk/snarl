#!/usr/bin/bash

case $2 in
    PRE-INSTALL)
	echo Creating snarl group ...
	groupadd snarl
	echo Creating snarl user ...
	useradd -g snarl -d /var/db/snarl -s /bin/false snarl
	echo Creating directories ...
	mkdir -p /var/db/snarl/ring
	mkdir -p /var/db/snarl/users
	mkdir -p /var/db/snarl/groups
	chown -R snarl:snarl /var/db/snarl
	mkdir -p /var/log/snarl/sasl
	chown -R snarl:snarl /var/log/snarl
	;;
    POST-INSTALL)
	echo Importing service ...
	svccfg import /opt/local/snarl/etc/snarl.xml
	;;
esac
