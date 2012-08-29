%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(snarl_group_state).

-include("snarl.hrl").

-export([
	 new/0,
	 name/2,
	 grant/2,
	 revoke/2,
	 add/2,
	 delete/2
	]).

new() ->
    #group{}.

name(Name, Group) ->
    Group#group{name = Name}.

grant(Permission, Group) ->
    Group#group{permissions = ordsets:add_element(Permission, Group#group.permissions)}.

revoke(Permission, Group) ->
    Group#group{permissions = ordsets:del_element(Permission, Group#group.permissions)}.

add(Group, Groups) ->
    ordsets:add_element(Group, Groups).

delete(Group, Groups) ->
    ordsets:del_element(Group, Groups).
