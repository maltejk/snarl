-module(snarl_map).


-export([new/0, merge/2, get/2, set/4, remove/3, value/1, split_path/1,
         from_orddict/2]).

-ignore_xref([get/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-define(SET, riak_dt_orswot).
-define(REG, riak_dt_lwwreg).
-define(MAP, riak_dt_map).
-define(COUNTER, riak_dt_pncounter).

new() ->
    riak_dt_map:new().

merge(A, B) ->
    riak_dt_map:merge(A, B).

get([K], M) ->
    Keys = riak_dt_map:value(keyset, M),
    case orddict:find(K, Keys) of
        {ok, T} ->
            value_(riak_dt_map:value({get, {K, T}}, M));
        E ->
            E
    end;

get([K | Ks], M) ->
    Keys = riak_dt_map:value(keyset, M),
    case orddict:find(K, Keys) of
        {ok, ?MAP} ->
            M1 = riak_dt_map:value({get_crdt, {K, ?MAP}}, M),
            get(Ks, M1);
        {ok, T} ->
            {error, {bad_type, K, T}};
        E ->
            E
    end;

get(K, M) ->
    get([K], M).

set(K, V, A, M) when not is_list(K) ->
    set([K], V, A, M);

set(Ks, [{_,_}|_] = D, A, M) ->
    lists:foldl(fun({KsI, V}, {ok, MAcc}) ->
                        set(Ks ++ KsI, V, A, MAcc)
                end, {ok, M}, flatten_orddict(D));

set(Ks, V, A, M) ->
    case split_path(Ks, [], M) of
        {ok, {[FirstNew | Missing], []}} ->
            Ops = nested_create([FirstNew | Missing], V),
            riak_dt_map:update({update, Ops}, A, M);
        {ok, {Missing, Existing}} ->
            Ops = nested_update(Existing,
                                nested_create(Missing, V)),
            riak_dt_map:update({update, Ops}, A, M);
        E ->
            E
    end.

split_path(P) when is_binary(P) ->
    re:split(P, "\\.");

split_path(P) ->
    P.

remove(Ks, A, M) when is_list(Ks) ->
    case remove_path(Ks, [], M) of
        {ok, {Path, K}} ->
            Ops = nested_update(Path, [{remove, K}]),
            riak_dt_map:update({update, Ops}, A, M);
        {ok, missing} ->
            M
    end;

remove(K, A, M) ->
    remove([K], A, M).

value(M) ->
    value_(riak_dt_map:value(M)).

from_orddict(D, Actor) ->
    lists:foldl(fun({Ks, V}, Map) ->
                        {ok, M1} = set(Ks, V, Actor, Map),
                        M1
                end, new(), flatten_orddict(D)).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

split_path([K | Ks], Existing, M) ->
    Keys = riak_dt_map:value(keyset, M),
    case orddict:find(K, Keys) of
        {ok, ?MAP} ->
            M1 = riak_dt_map:value({get_crdt, {K, ?MAP}}, M),
            split_path(Ks, [K | Existing], M1);
        {ok, T} when
              Ks =/= [] ->
            {error, not_a_map, T, lists:reverse([K | Existing])};
        {ok, _} ->
            {ok, {[K], lists:reverse(Existing)}};
        _ ->
            {ok, {[K | Ks], lists:reverse(Existing)}}
    end.

remove_path([K], Path, M) ->
    Keys = riak_dt_map:value(keyset, M),
    case orddict:find(K, Keys) of
        {ok, T} ->
            {ok, {lists:reverse(Path), {K, T}}};
        _ ->
            {ok, missing}
    end;

remove_path([K | Ks], Path, M) ->
    Keys = riak_dt_map:value(keyset, M),
    case orddict:find(K, Keys) of
        {ok, ?MAP} ->
            M1 = riak_dt_map:value({get_crdt, {K, ?MAP}}, M),
            remove_path(Ks, [K | Path], M1);
        _ ->
            {ok, missing}
    end.

nested_update([], U) ->
    U;

nested_update([K], U) ->
    [{update, {K, ?MAP}, {update, U}}];

nested_update([K | Ks], U) ->
    [{update, {K, ?MAP}, {update, nested_update(Ks, U)}}].

nested_create([K], V) ->
    {Type, Us} = update_from_value(V),
    Field = {K, Type},
    [{add, Field} |
     [{update, Field, U} || U <- Us]];

nested_create([K | Ks], V) ->
    Field = {K, ?MAP},
    [{add, Field},
     {update, Field, {update, nested_create(Ks, V)}}].

update_from_value({custom, Type, Actions}) when is_list(Actions)->
    {Type, Actions};

update_from_value({custom, Type, Action}) ->
    {Type, [Action]};

update_from_value({reg, V}) ->
    update_from_value({custom, ?REG, {assign, V}});

update_from_value({set, V}) when is_list(V) ->
    update_from_value({set, {add_all, V}});

update_from_value({set, {add_all, V}}) ->
    update_from_value({custom, ?SET, {add_all, V}});

update_from_value({set, {add, V}}) ->
    update_from_value({custom, ?SET, {add, V}});

update_from_value({set, {remove, V}}) ->
    update_from_value({custom, ?SET, {remove, V}});

update_from_value({set, V}) ->
    update_from_value({set, {add, V}});

update_from_value({counter, inc}) ->
    update_from_value({custom, ?COUNTER, {increment, 1}});

update_from_value({counter, dec}) ->
    update_from_value({custom, ?COUNTER, {decrement, 1}});

update_from_value({counter, V}) when V >= 0->
    update_from_value({custom, ?COUNTER, {increment, V}});

update_from_value({counter, V}) when V =< 0->
    update_from_value({custom, ?COUNTER, {decrement, -V}});

update_from_value(V) ->
    update_from_value({reg, V}).

value_(N) when is_number(N) ->
    N;

value_(B) when is_binary(B) ->
    B;

value_([{{_,_}, _} | _] = L) ->
    orddict:from_list([{K, value_(V)} || {{K,_}, V} <- L]);

value_(L) when is_list(L) ->
    [value_(V) || V <- L];

value_(V) ->
    V.

flatten_orddict(D) ->
    [{lists:reverse(Ks), V} || {Ks, V} <- flatten_orddict([], D, [])].
flatten_orddict(Prefix, [{K, [{_,_}|_] = V} | R], Acc) ->
    Acc1 = flatten_orddict([K | Prefix], V, Acc),
    flatten_orddict(Prefix, R, Acc1);
flatten_orddict(Prefix, [{K, V} | R], Acc) ->
    flatten_orddict(Prefix, R, [{[K | Prefix], V} | Acc]);
flatten_orddict(_, [], Acc) ->
    Acc.

%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

flatten_orddict_test() ->
    O1 = [{k, v}],
    F1 = [{[k], v}],
    O2 = [{k1, [{k11, v11}]}, {k2, v2}],
    F2 = [{[k2], v2}, {[k1, k11], v11}],
    O3 = [{k1, [{k11, [{k111, v111}]}]}, {k2, v2}],
    F3 = [{[k2], v2}, {[k1, k11, k111], v111}],
    ?assertEqual(F1, flatten_orddict(O1)),
    ?assertEqual(F2, flatten_orddict(O2)),
    ?assertEqual(F3, flatten_orddict(O3)),
    ok.

from_orddict_test() ->
    O1 = [{k, v}],
    M1 = from_orddict(O1, none),
    O2 = [{k1, [{k11, v11}]}, {k2, v2}],
    M2 = from_orddict(O2, none),
    O3 = [{k1, [{k11, [{k111, v111}]}]}, {k2, v2}],
    M3 = from_orddict(O3, none),
    ?assertEqual(O1, value(M1)),
    ?assertEqual(O2, value(M2)),
    ?assertEqual(O3, value(M3)),
    ok.

adding_mapo_test() ->
    M = snarl_map:new(),
    {ok, M1} = snarl_map:set(k, [{k1, v1}], a, M),
    ?assertEqual([{k, [{k1, v1}]}], snarl_map:value(M1)),
    ?assertEqual(v1, snarl_map:get([k, k1], M1)),
    ok.

reg_test() ->
    M = snarl_map:new(),
    {ok, M1} = snarl_map:set(k, v, a, M),
    {ok, M2} = snarl_map:set(k, v1, a, M1),
    ?assertEqual(v, snarl_map:get(k, M1)),
    ?assertEqual(v1, snarl_map:get(k, M2)),
    ok.

counter_test() ->
    M = snarl_map:new(),
    {ok, M1} = snarl_map:set(k, {counter, 3}, a, M),
    {ok, M2} = snarl_map:set(k, {counter, -2}, a, M1),
    ?assertEqual(3, snarl_map:get(k, M1)),
    ?assertEqual(1, snarl_map:get(k, M2)),
    ok.

set_test() ->
    M = snarl_map:new(),
    {ok, M1} = snarl_map:set(k, {set, 3}, a, M),
    {ok, M2} = snarl_map:set(k, {set, 2}, a, M1),
    {ok, M3} = snarl_map:set(k, {set, [1,4]}, a, M2),
    {ok, M4} = snarl_map:set(k, {set, {remove, 3}}, a, M3),

    ?assertEqual([3], snarl_map:get(k, M1)),
    ?assertEqual([2,3], snarl_map:get(k, M2)),
    ?assertEqual([1,2,3,4], snarl_map:get(k, M3)),
    ?assertEqual([1,2,4], snarl_map:get(k, M4)),
    ok.

nested_reg_test() ->
    M = snarl_map:new(),
    {ok, M1} = snarl_map:set([o, k], v, a, M),
    {ok, M2} = snarl_map:set([o, k], v1, a, M1),
    ?assertEqual(v, snarl_map:get([o, k], M1)),
    ?assertEqual(v1, snarl_map:get([o, k], M2)),
    ok.

nested_counter_test() ->
    M = snarl_map:new(),
    {ok, M1} = snarl_map:set([o, k], {counter, 3}, a, M),
    {ok, M2} = snarl_map:set([o, k], {counter, -2}, a, M1),
    ?assertEqual(3, snarl_map:get([o, k], M1)),
    ?assertEqual(1, snarl_map:get([o, k], M2)),
    ok.

delete_test() ->
    M = snarl_map:new(),
    {ok, M1} = snarl_map:set(k, v, a, M),
    {ok, M2} = snarl_map:set([o, k], v1, a, M1),
    {ok, M3} = snarl_map:remove(k, a, M2),
    {ok, M4} = snarl_map:remove(o, a, M2),
    {ok, M5} = snarl_map:remove([o, k], a, M2),
    ?assertEqual(v, snarl_map:get(k, M1)),
    ?assertEqual(v1, snarl_map:get([o, k], M2)),
    ?assertEqual([{k, v}, {o, [{k, v1}]}],
                 snarl_map:value(M2)),
    ?assertEqual([{o, [{k, v1}]}],
                 snarl_map:value(M3)),
    ?assertEqual([{k, v}],
                 snarl_map:value(M4)),
    ?assertEqual([{k, v}, {o, []}],
                 snarl_map:value(M5)),
    ok.

-endif.
