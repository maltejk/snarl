%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(snarl_user_state).

-include("snarl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-export([
         load/1,
         new/0,
         name/2,
         passwd/2,
         grant/2,
         revoke/2,
         join/2,
         leave/2,
         add/2,
         delete/2,
         set_resource/3,
         get_resource/2,
         claim_resource/4,
         free_resource/3,
         get_free_resource/2,
         get_resource_stat/1
        ]).

-ignore_xref([
              claim_resource/4,
              free_resource/3,
              get_resource/2,
              set_resource/3,
              grant/2,
              join/2,
              leave/2,
              passwd/2,
              revoke/2
             ]).

load(#user{} = User) ->
    User0 = jsxd:new(),
    User1 = jsxd:set(<<"version">>, <<"0.1.0">>, User0),
    User2 = jsxd:set(<<"name">>, User#user.name, User1),
    User3 = jsxd:set(<<"password">>, User#user.passwd, User2),
    User4 = jsxd:set(<<"permissions">>, User#user.permissions, User3),
    jsxd:set(<<"groups">>, User#user.groups, User4);

load(User) ->
    User.

new() ->
    jsxd:new().

name(Name, User) ->
    jsxd:set(<<"name">>, Name, User).

passwd(Passwd, User) ->
    {ok, Name} = jsxd:get(<<"name">>, User),
    jsxd:set(<<"password">>, crypto:sha([Name, Passwd]), User).

grant(Permission, User) ->
    jsxd:update(<<"permissions">>,
                fun (Ps) ->
                        ordsets:add_element(Permission, Ps)
                end, [Permission], User).

revoke(Permission, User) ->
    jsxd:update(<<"permissions">>,
                fun (Ps) ->
                        ordsets:del_element(Permission, Ps)
                end, [], User).

join(Group, User) ->
    jsxd:update(<<"groups">>,
                fun (Gs) ->
                        ordsets:add_element(Group, Gs)
                end, [Group], User).

leave(Group, User) ->
    jsxd:update(<<"groups">>,
                fun (Gs) ->
                        ordsets:del_element(Group, Gs)
                end, [], User).

add(User, Users) ->
    ordsets:add_element(User, Users).

delete(User, Users) ->
    ordsets:del_element(User, Users).

set_resource(Resource, Granted, User) ->
    jsxd:set([<<"resources">>, Resource, <<"granted">>], Granted, User).

claim_resource(Resource, ID, Ammount, User) ->
    jsxd:set([<<"resources">>, Resource, <<"claims">>, ID], Ammount, User).

free_resource(Resource, ID, User) ->
    jsxd:delete([<<"resources">>, Resource, <<"claims">>, ID], User).

get_resource_stat(User) ->
    jsxd:map(fun(_ID, Res) ->
                     {ok, Granted} = jsxd:get(<<"granted">>, Res),
                     Used = jsxd:fold(fun(_, Ammount, Sum) ->
                                              Ammount + Sum
                                      end, 0, jsxd:get(<<"claims">>, [], Res)),
                     [{<<"granted">>, Granted},
                      {<<"used">>, Used}]
             end, jsxd:get(<<"resources">>, [], User)).

get_resource(Resource, User) ->
    {ok, Res} = jsxd:get([<<"resources">>, Resource], User),
    Res.

get_free_resource(Resource, User) ->
    {ok, Granted} = jsxd:get([<<"resources">>, Resource, <<"granted">>], User),
    Used = jsxd:fold(fun(_, Ammount, Sum) ->
                             Ammount + Sum
                     end, 0, jsxd:get([<<"resources">>, Resource, <<"claims">>], [], User)),
    Granted - Used.

-ifdef(TEST).
name_test() ->
    Name0 = <<"Test0">>,
    User0 = new(),
    User1 = name(Name0, User0),
    Name1 = <<"Test1">>,
    User2 = name(Name1, User1),
    ?assertEqual({ok, Name0},
                 jsxd:get(<<"name">>, User1)),
    ?assertEqual({ok, Name1},
                 jsxd:get(<<"name">>, User2)).

passwd_test() ->
    Name = "Test",
    Passwd = "Test",
    Hash = crypto:sha([Name, Passwd]),
    User0 = new(),
    User1 = name(Name, User0),
    User2 = passwd(Passwd, User1),
    ?assertEqual({ok, Hash},
                 jsxd:get(<<"password">>, User2)).

permissions_test() ->
    P0 = [<<"P0">>],
    P1 = [<<"P1">>],
    User0 = new(),
    User1 = grant(P0, User0),
    User2 = grant(P1, User1),
    User3 = grant(P0, User2),
    User4 = revoke(P0, User3),
    User5 = revoke(P1, User3),
    ?assertEqual({ok, [P0]},
                 jsxd:get(<<"permissions">>, User1)),
    ?assertEqual({ok, [P0, P1]},
                 jsxd:get(<<"permissions">>, User2)),
    ?assertEqual({ok, [P0, P1]},
                 jsxd:get(<<"permissions">>, User3)),
    ?assertEqual({ok, [P1]},
                 jsxd:get(<<"permissions">>, User4)),
    ?assertEqual({ok, [P0]},
                 jsxd:get(<<"permissions">>, User5)).

groups_test() ->
    G0 = "G0",
    G1 = "G1",
    User0 = new(),
    User1 = join(G0, User0),
    User2 = join(G1, User1),
    User3 = join(G0, User2),
    User4 = leave(G0, User3),
    User5 = leave(G1, User3),
    ?assertEqual({ok, [G0]},
                 jsxd:get(<<"groups">>, User1)),
    ?assertEqual({ok, [G0, G1]},
                 jsxd:get(<<"groups">>, User2)),
    ?assertEqual({ok, [G0, G1]},
                 jsxd:get(<<"groups">>, User3)),
    ?assertEqual({ok, [G1]},
                 jsxd:get(<<"groups">>, User4)),
    ?assertEqual({ok, [G0]},
                 jsxd:get(<<"groups">>, User5)).

resource_test() ->
    User0 = new(),
    User1 = set_resource(<<"cookies">>, 10, User0),
    User2 = set_resource(<<"cookies">>, 20, User1),
    User3 = set_resource(<<"cakes">>, 42, User2),

    R0 = get_resource(<<"cookies">>, User1),
    R1 = get_resource(<<"cookies">>, User3),
    R2 = get_resource(<<"cakes">>, User3),

    ?assertEqual({ok, 10}, jsxd:get(<<"granted">>, R0)),
    ?assertEqual({ok, 20}, jsxd:get(<<"granted">>, R1)),
    ?assertEqual({ok, 42}, jsxd:get(<<"granted">>, R2)).

resource_stat_test() ->
    User0 = new(),
    User1 = set_resource(<<"cookies">>, 10, User0),
    User2 = claim_resource(<<"cookies">>, <<"c1">>, 1, User1),
    User3 = claim_resource(<<"cookies">>, <<"c2">>, 2, User2),
    User4 = free_resource(<<"cookies">>, <<"c1">>, User3),
    User5 = free_resource(<<"cookies">>, <<"c2">>, User3),
    User6 = free_resource(<<"cookies">>, <<"c1">>, User4),
    User7 = set_resource(<<"cake">>, 5, User6),
    User8 = claim_resource(<<"cake">>, <<"c1">>, 1, User7),

    ?assertEqual([{<<"cookies">>,
                   [{<<"granted">>,10},{<<"used">>,0}]}],
                 get_resource_stat(User1)),
    ?assertEqual([{<<"cookies">>,
                   [{<<"granted">>,10},{<<"used">>,1}]}],
                 get_resource_stat(User2)),
    ?assertEqual([{<<"cookies">>,
                   [{<<"granted">>,10},{<<"used">>,3}]}],
                 get_resource_stat(User3)),
    ?assertEqual([{<<"cookies">>,
                   [{<<"granted">>,10},{<<"used">>,2}]}],
                 get_resource_stat(User4)),
    ?assertEqual([{<<"cookies">>,
                   [{<<"granted">>,10},{<<"used">>,1}]}],
                 get_resource_stat(User5)),
    ?assertEqual([{<<"cookies">>,
                   [{<<"granted">>,10},{<<"used">>,2}]}],
                 get_resource_stat(User6)),
    ?assertEqual([{<<"cookies">>,
                   [{<<"granted">>,10},{<<"used">>,2}]},
                  {<<"cake">>,
                   [{<<"granted">>,5},{<<"used">>,1}]}],
                 get_resource_stat(User8)).

resource_claim_test() ->
    User0 = new(),
    User1 = set_resource(<<"cookies">>, 10, User0),
    User2 = claim_resource(<<"cookies">>, <<"c1">>, 1, User1),
    User3 = claim_resource(<<"cookies">>, <<"c2">>, 2, User2),
    User4 = free_resource(<<"cookies">>, <<"c1">>, User3),
    User5 = free_resource(<<"cookies">>, <<"c2">>, User3),
    User6 = free_resource(<<"cookies">>, <<"c1">>, User4),

    ?assertEqual(10, get_free_resource(<<"cookies">>, User1)),
    ?assertEqual(9, get_free_resource(<<"cookies">>, User2)),
    ?assertEqual(7, get_free_resource(<<"cookies">>, User3)),
    ?assertEqual(8, get_free_resource(<<"cookies">>, User4)),
    ?assertEqual(9, get_free_resource(<<"cookies">>, User5)),
    ?assertEqual(8, get_free_resource(<<"cookies">>, User6)).

-endif.
