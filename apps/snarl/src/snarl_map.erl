-module(snarl_map).

-export([new/0, merge/2, get/2, set/5, remove/3, value/1, split_path/1,
         from_orddict/3]).

-ifdef(TEST).
-export([flatten_orddict/1]).
-endif.

-ignore_xref([get/2]).


-define(SET, riak_dt_orswot).
-define(REG, riak_dt_lwwreg).
-define(MAP, riak_dt_map).
-define(COUNTER, riak_dt_pncounter).

-spec new() -> riak_dt_map:map().

new() ->
    riak_dt_map:new().

merge(A, B) ->
    riak_dt_map:merge(A, B).

-spec get(Keys::[binary()]|binary(), Map::riak_dt_map:map()) ->
                 term().

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
            {error, E}
    end;

get(K, M) ->
    get([K], M).

-spec set(Key::[binary()]|binary(), Value::term(),
          Actor::atom(), Timestamp::non_neg_integer(),
          Map::riak_dt_map:map()) ->
                 {ok, riak_dt_map:map()}.

set(K, V, A, T, M) when not is_list(K) ->
    set([K], V, A, T, M);

set(Ks, [{_,_}|_] = D, A, T, M) ->
    lists:foldl(fun({KsI, V}, {ok, MAcc}) ->
                        set(Ks ++ KsI, V, A, T, MAcc)
                end, {ok, M}, flatten_orddict(D));

set(Ks, V, A, T, M) ->
    case split_path(Ks, [], M) of
        {ok, {[FirstNew | Missing], []}} ->
            Ops = nested_create([FirstNew | Missing], V, T),
            riak_dt_map:update({update, Ops}, A, M);
        {ok, {Missing, Existing}} ->
            Ops = nested_update(Existing,
                                nested_create(Missing, V, T)),
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
            {ok, M}
    end;

remove(K, A, M) ->
    remove([K], A, M).

value(M) ->
    value_(riak_dt_map:value(M)).

-spec from_orddict(D::orddict:orddict(),
                   Actor::term(),
                   Timestamp::non_neg_integer()) ->
                          riak_dt_map:map().

from_orddict(D, Actor, Timestamp) ->
    lists:foldl(fun({Ks, V}, Map) ->
                        {ok, M1} = set(Ks, V, Actor, Timestamp, Map),
                        M1
                end, new(), flatten_orddict(D)).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec split_path([binary()], [binary()], riak_dt_map:map()) ->
                        {error, not_a_map, term(), [binary()]} |
                        {ok, {[binary()], [binary()]}}.
%%split_path([K | Ks], Existing, M) when is_list(Ks) ->
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

nested_create([K], V, T) ->
    {Type, Us} = update_from_value(V, T),
    Field = {K, Type},
    [{add, Field} |
     [{update, Field, U} || U <- Us]];

nested_create([K | Ks], V, T) ->
    Field = {K, ?MAP},
    [{add, Field},
     {update, Field, {update, nested_create(Ks, V, T)}}].

update_from_value({custom, Type, Actions}, _) when is_list(Actions)->
    {Type, Actions};

update_from_value({custom, Type, Action}, _) ->
    {Type, [Action]};

update_from_value({reg, V}, T) ->
    update_from_value({custom, ?REG, {assign, V}}, T);

update_from_value({set, V}, T) when is_list(V) ->
    update_from_value({set, {add_all, V}}, T);

update_from_value({set, {add_all, V}}, T) ->
    update_from_value({custom, ?SET, {add_all, V}}, T);

update_from_value({set, {add, V}}, T) ->
    update_from_value({custom, ?SET, {add, V}}, T);

update_from_value({set, {remove, V}}, T) ->
    update_from_value({custom, ?SET, {remove, V}}, T);

update_from_value({set, V}, T) ->
    update_from_value({set, {add, V}}, T);

update_from_value({counter, inc}, T) ->
    update_from_value({custom, ?COUNTER, {increment, 1}}, T);

update_from_value({counter, dec}, T) ->
    update_from_value({custom, ?COUNTER, {decrement, 1}}, T);

update_from_value({counter, V}, T) when V >= 0->
    update_from_value({custom, ?COUNTER, {increment, V}}, T);

update_from_value({counter, V}, T) when V =< 0->
    update_from_value({custom, ?COUNTER, {decrement, -V}}, T);

update_from_value(V, T) ->
    update_from_value({reg, V}, T).

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
