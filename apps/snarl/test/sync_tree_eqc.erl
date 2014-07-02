-module(sync_tree_eqc).

-ifdef(TEST).
-ifdef(EQC).

-include_lib("fqc/include/fqc.hrl").

-compile(export_all).
-define(M, ?MODULE).
-define(S, snarl_sync).

kv() ->
    {non_blank_string(), int()}.

tree() ->
    ?SIZED(Size, tree(Size)).

tree(Size) ->
    ?LAZY(oneof([{call, ?M, init, []} || Size == 0] ++
                    [?LETSHRINK(
                        [U], [tree(Size - 1)],
                        oneof([
                               {call, ?M, add_left, [U, kv()]},
                               {call, ?M, add_right, [U, kv()]},
                               {call, ?M, add_both, [U, kv()]}
                              ])) || Size > 0])).

add_left({L, R}, KV) ->
    {[KV | L], R}.

add_right({L, R}, KV) ->
    {L, [KV | R]}.

add_both({L, R}, KV) ->
    {[KV | L], [KV | R]}.

init() ->
    {[], []}.

prop_add_diff() ->
    ?FORALL(T, tree(),
            begin
                {L, R} = eval(T),
                {D, RO, LO} = ?S:split_trees(L, R),
                M = {lists:sort([a | D]), lists:sort(RO), lists:sort(LO)},
                L1 = [{a, 2} | L],
                R1 = [{a, 1} | R],
                {D1, RO1, LO1} = ?S:split_trees(L1, R1),
                ?WHENFAIL(io:format(user,
                                    "[0] L: ~p / R: ~p -> ~p ~n"
                                    "[1] L: ~p / R: ~p -> ~p == ~p~n",
                                    [L, R, {D, RO, LO}, L1, R1, M, {D1, RO1, LO1}]),
                          M == {lists:sort(D1), lists:sort(RO1), lists:sort(LO1)})
            end).

prop_add_left() ->
    ?FORALL(T, tree(),
            begin
                {L, R} = eval(T),
                {D, RO, LO} = ?S:split_trees(L, R),
                M = {lists:sort(D), lists:sort(RO), lists:sort([a | LO])},
                L1 = [{a, 1} | L],
                R1 = R,
                {D1, RO1, LO1} = ?S:split_trees(L1, R1),
                ?WHENFAIL(io:format(user,
                                    "[0] L: ~p / R: ~p -> ~p ~n"
                                    "[1] L: ~p / R: ~p -> ~p == ~p~n",
                                    [L, R, {D, RO, LO}, L1, R1, M, {D1, RO1, LO1}]),
                          M == {lists:sort(D1), lists:sort(RO1), lists:sort(LO1)})
            end).

prop_add_right() ->
    ?FORALL(T, tree(),
            begin
                {L, R} = eval(T),
                {D, RO, LO} = ?S:split_trees(L, R),
                M = {lists:sort(D), lists:sort([a | RO]), lists:sort(LO)},
                L1 = L,
                R1 = [{a, 1} | R],
                {D1, RO1, LO1} = ?S:split_trees(L1, R1),
                ?WHENFAIL(io:format(user,
                                    "[0] L: ~p / R: ~p -> ~p ~n"
                                    "[1] L: ~p / R: ~p -> ~p == ~p~n",
                                    [L, R, {D, RO, LO}, L1, R1, M, {D1, RO1, LO1}]),
                          M == {lists:sort(D1), lists:sort(RO1), lists:sort(LO1)})
            end).
prop_add_equal() ->
    ?FORALL(T, tree(),
            begin
                {L, R} = eval(T),
                {D, RO, LO} = ?S:split_trees(L, R),
                M = {lists:sort(D), lists:sort(RO), lists:sort(LO)},
                L1 = [{a, 1} | L],
                R1 = [{a, 1} | R],
                {D1, RO1, LO1} = ?S:split_trees(L1, R1),
                ?WHENFAIL(io:format(user,
                                    "[0] L: ~p / R: ~p -> ~p ~n"
                                    "[1] L: ~p / R: ~p -> ~p == ~p~n",
                                    [L, R, {D, RO, LO}, L1, R1, M, {D1, RO1, LO1}]),
                          M == {lists:sort(D1), lists:sort(RO1), lists:sort(LO1)})
            end).

-endif.
-endif.
