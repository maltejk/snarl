Snarl
=====

Build status (master): [![Build Status](https://secure.travis-ci.org/project-fifo/snarl.png?branch=master)](http://travis-ci.org/project-fifo/snarl)

Build status (dev): [![Build Status](https://secure.travis-ci.org/project-fifo/snarl.png?branch=dev)](http://travis-ci.org/project-fifo/snarl)

Snarl is a right management server build on top of [riak_core](https://github.com/basho/riak_core/). The permission architecture is as following:

Each permission consists of a list of values, where the values **'…'** and **'_'** (both Erlang atoms) have a special meaning.

* '...' matches one, more or no values.
* '_' matches exactly one value.
* everything else just matches itself.

Examples
--------
[some, cool, permission] matches:

* [some, cool, permission]
* [some, '_', permission]
* ['\_', '_', permission]
* ['...', permission]
* [some, '...', permission]
* [some, '...']

Interface
---------
Snarl publishes it's servers via mDNS as

```
_snarl._zmq._tcp.<domain>
```

The txt record of the announcements contains:

* **server**: ip of the server
* **port**: port of ZMQ

Message
-------
Each message is passed as a BERT encoded Erlang terms.

User Functions
* {user, list} -> [Name::binary()]
* {user, get, User|Token} -> {ok, {user, Name::binary(), Password::binary(), Permissions, GroupNames}} | not_found
* {user, add, User} -> ok | duplicate 
* {user, delete, User} -> ok | not_found
* {user, grant, User, Permission} -> ok | not_found
* {user, revoke, User, Permission} -> ok | not_found
* {user, passwd, User, Pass} -> ok | not_found
* {user, join, User, Group} -> ok | not_found
* {user, leave, User, Group} -> ok | not_found
* {user, auth, User, Pass} -> {ok, Token} | false
* {user, allowed, User|Token, Permission} -> true | false


Group Functions
* {group, list} -> [Name::binary()]
* {group, get, Group} -> {ok, {group, Name::binary(), Permissions}} | not_found
* {group, add, Group} -> ok | duplicate
* {group, delete, Group} -> ok | not_found
* {group, grant, Group, Permission} -> ok | not_found
* {group, revoke, Group, Permission} -> ok | not_found

Credits
-------
If you want to learn something about riak_core I can recommend [rzezeski's working blog](https://github.com/rzezeski/try-try-try) the implementation is heavily build on top of the content provided there.