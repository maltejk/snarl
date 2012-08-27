%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(snarl_user_state).

-include("snarl.hrl").

-export([
	 new/0,
	 name/2,
	 passwd/2,
	 grant/2,
	 revoke/2,
	 join/2,
	 leave/2
	]).


new() ->
    #user{}.

name(Name, User) ->
    User#user{name = Name}.

passwd(Passwd, User) ->
    User#user{passwd = crypto:sha([User#user.name, Passwd])}.

grant(Permission, User) ->
    User#user{permissions = ordsets:add_element(Permission, User#user.permissions)}.

revoke(Permission, User) ->
    User#user{permissions = ordsets:del_element(Permission, User#user.permissions)}.

join(Group, User) ->
    User#user{groups = ordsets:add_element(Group, User#user.groups)}.

leave(Group, User) ->
    User#user{groups = ordsets:del_element(Group, User#user.groups)}.



    
	


