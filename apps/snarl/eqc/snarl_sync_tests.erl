-module(snarl_sync_tests).
-define(M, snarl_sync).

-include_lib("eunit/include/eunit.hrl").

split_tree_empty_test() ->
    L = [],
    R = [],
    ?assertEqual({[], [], []}, ?M:split_trees(L, R)).

split_tree_equal_test() ->
    L = [{a, 1}],
    R = [{a, 1}],
    ?assertEqual({[], [], []}, ?M:split_trees(L, R)).

split_tree_diff_test() ->
    L = [{a, 1}],
    R = [{a, 2}],
    ?assertEqual({[a], [], []}, ?M:split_trees(L, R)).

split_tree_get_test() ->
    L = [{a, 1}],
    R = [{a, 1}, {b, 1}],
    ?assertEqual({[], [b], []}, ?M:split_trees(L, R)).

split_tree_push_test() ->
    L = [{a, 1}, {b, 1}],
    R = [{a, 1}],
    ?assertEqual({[], [], [b]}, ?M:split_trees(L, R)).

split_tree_test() ->
    L = [{a, 1}, {b, 2}, {c, 1}, {d, 1}],
    R = [{a, 1}, {b, 1}, {d, 1}, {e, 1}],
    ?assertEqual({[b], [e], [c]}, ?M:split_trees(L, R)).
