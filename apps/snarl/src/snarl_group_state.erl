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
         revoke/2
        ]).

-ignore_xref([
              name/2,
              grant/2,
              revoke/2
             ]).


load(#group{} = Group) ->
    Group0 = jsxd:set(<<"version">>, <<"0.1.0">>, jsxd:new()),
    jsxd:set(<<"permissions">>, Group#group.permissions, Group0);

load(User) ->
    User.

new() ->
    jsxd:new().

name(Name, Group) ->
    jsxd:set(<<"name">>, Name, Group).

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
