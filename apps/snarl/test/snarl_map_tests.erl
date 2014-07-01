-module(snarl_map_tests).
-define(M, snarl_map).
-include_lib("eunit/include/eunit.hrl").

flatten_orddict_test() ->
    O1 = [{k, v}],
    F1 = [{[k], v}],
    O2 = [{k1, [{k11, v11}]}, {k2, v2}],
    F2 = [{[k2], v2}, {[k1, k11], v11}],
    O3 = [{k1, [{k11, [{k111, v111}]}]}, {k2, v2}],
    F3 = [{[k2], v2}, {[k1, k11, k111], v111}],
    ?assertEqual(F1, ?M:flatten_orddict(O1)),
    ?assertEqual(F2, ?M:flatten_orddict(O2)),
    ?assertEqual(F3, ?M:flatten_orddict(O3)),
    ok.

from_orddict_test() ->
    O1 = [{k, v}],
    M1 = ?M:from_orddict(O1, none, 0),
    O2 = [{k1, [{k11, v11}]}, {k2, v2}],
    M2 = ?M:from_orddict(O2, none, 0),
    O3 = [{k1, [{k11, [{k111, v111}]}]}, {k2, v2}],
    M3 = ?M:from_orddict(O3, none, 0),
    ?assertEqual(O1, ?M:value(M1)),
    ?assertEqual(O2, ?M:value(M2)),
    ?assertEqual(O3, ?M:value(M3)),
    ok.

adding_mapo_test() ->
    M = ?M:new(),
    {ok, M1} = ?M:set(k, [{k1, v1}], a, 0, M),
    ?assertEqual([{k, [{k1, v1}]}], ?M:value(M1)),
    ?assertEqual(v1, ?M:get([k, k1], M1)),
    ok.

reg_test() ->
    M = ?M:new(),
    {ok, M1} = ?M:set(k, v, 0, a, M),
    {ok, M2} = ?M:set(k, v1, 1, a, M1),
    ?assertEqual(v, ?M:get(k, M1)),
    ?assertEqual(v1, ?M:get(k, M2)),
    ok.

counter_test() ->
    M = ?M:new(),
    {ok, M1} = ?M:set(k, {counter, 3}, a, 0, M),
    {ok, M2} = ?M:set(k, {counter, -2}, a, 1, M1),
    ?assertEqual(3, ?M:get(k, M1)),
    ?assertEqual(1, ?M:get(k, M2)),
    ok.

set_test() ->
    M = ?M:new(),
    {ok, M1} = ?M:set(k, {set, 3}, a, 0, M),
    {ok, M2} = ?M:set(k, {set, 2}, a, 1, M1),
    {ok, M3} = ?M:set(k, {set, [1,4]}, a, 2, M2),
    {ok, M4} = ?M:set(k, {set, {remove, 3}}, a, 3, M3),

    ?assertEqual([3], ?M:get(k, M1)),
    ?assertEqual([2,3], ?M:get(k, M2)),
    ?assertEqual([1,2,3,4], ?M:get(k, M3)),
    ?assertEqual([1,2,4], ?M:get(k, M4)),
    ok.

nested_reg_test() ->
    M = ?M:new(),
    {ok, M1} = ?M:set([o, k], v, a, 0, M),
    {ok, M2} = ?M:set([o, k], v1, a, 1, M1),
    ?assertEqual(v, ?M:get([o, k], M1)),
    ?assertEqual(v1, ?M:get([o, k], M2)),
    ok.

nested_counter_test() ->
    M = ?M:new(),
    {ok, M1} = ?M:set([o, k], {counter, 3}, a, 0, M),
    {ok, M2} = ?M:set([o, k], {counter, -2}, a, 1, M1),
    ?assertEqual(3, ?M:get([o, k], M1)),
    ?assertEqual(1, ?M:get([o, k], M2)),
    ok.

delete_test() ->
    M = ?M:new(),
    {ok, M1} = ?M:set(k, v, a, 0, M),
    {ok, M2} = ?M:set([o, k], v1, a, 1, M1),
    {ok, M3} = ?M:remove(k, a, M2),
    {ok, M4} = ?M:remove(o, a, M2),
    {ok, M5} = ?M:remove([o, k], a, M2),
    ?assertEqual(v, ?M:get(k, M1)),
    ?assertEqual(v1, ?M:get([o, k], M2)),
    ?assertEqual([{k, v}, {o, [{k, v1}]}],
                 ?M:value(M2)),
    ?assertEqual([{o, [{k, v1}]}],
                 ?M:value(M3)),
    ?assertEqual([{k, v}],
                 ?M:value(M4)),
    ?assertEqual([{k, v}, {o, []}],
                 ?M:value(M5)),
    ok.
