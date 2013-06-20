%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(snarl_group_state).

-include("snarl.hrl").

-export([
         load/1,
         new/0,
         name/2,
         grant/2,
         uuid/2,
         set/3,
         revoke/2
        ]).

-ignore_xref([
              uuid/2,
              name/2,
              grant/2,
              revoke/2
             ]).

load(User) ->
    User.

new() ->
    jsxd:new().

name(Name, Group) ->
    jsxd:set(<<"name">>, Name, Group).

uuid(UUID, Group) ->
    jsxd:set(<<"uuid">>, UUID, Group).

grant(Permission, Group) ->
    jsxd:update(<<"permissions">>,
                fun (Ps) ->
                        ordsets:add_element(Permission, Ps)
                end, [Permission], Group).

revoke(Permission, Group) ->
    jsxd:update(<<"permissions">>,
                fun (Ps) ->
                        ordsets:del_element(Permission, Ps)
                end, [Permission], Group).

set(Attribute, delete, Group) ->
    jsxd:delete(Attribute, Group);

set(Attribute, Value, Group) ->
    jsxd:set(Attribute, Value, Group).
