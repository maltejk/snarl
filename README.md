Snarl
=====

Build status (master): [![Build Status](https://secure.travis-ci.org/project-fifo/snarl.png?branch=master)](http://travis-ci.org/project-fifo/snarl)

Build status (dev): [![Build Status](https://secure.travis-ci.org/project-fifo/snarl.png?branch=dev)](http://travis-ci.org/project-fifo/snarl)

Snarl is a right management server build on top of [riak_core](https://github.com/basho/riak_core/). The permission architecture is as following:

Each permission consists of a list of values, where the values '...' and '_' (both Erlang atoms) have a special meaning.

* '...' matches one, more or no values.
* '_' matches exactly one value.
* everything else just matches itself.

Examples
--------

[some, cool, permission] matches:

* [some, cool, permission]
* [some, '_', permission]
* ['_', '_', permission]
* ['...', permission]
* [some, '...', permission]
* [some, '...']

Interface
---------


Snarl publishes it's servers via mDNS as

```
_snarl._zmq._tcp.<domain>
```

the txt record of the annoucements contains:

* *server*: ip of the server
* *port*: port of ZMQ

Message
-------


User Rleated
* {user, list} -> [user()]
* {user, get, User} -> {ok, {user, Name, Password, Permissions, Groups}} | not_found
* {user, add, User} -> ok
* {user, auth, User, Pass} -> true | false
* {user, allowed, User, Permission} -> true/false
* {user, delete, User} -> ok 
* {user, passwd, User, Pass} -> ok | not_found
* {user, join, User, Group} -> ok | not_found
* {user, leave, User, Group} -> ok | not_found
* {user, grant, User, Permission} -> ok | not_found
* {user, revoke, User, Permission} -> ok | not_found


Group Functions
* {group, list} -> [user()]
* {group, get, Group} -> {ok, {group, Name, Permissions}} | not_found
* {group, add, Group} -> ok | not_found
* {group, delete, Group} -> ok | not_found
* {group, grant, Group, Permission} -> ok | not_found
* {group, revoke, Group, Permission} -> ok | not_found