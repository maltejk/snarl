-module(snarl_eqc).

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile(export_all).

str() ->
    ?SUCHTHAT(L, list(choose($a, $z)), L =/= []).
atom() ->
    elements([a,b,c,undefined]).

prop_ensure_str() ->
    ?FORALL(E, oneof([binary(), int(), real(), atom(), str()]),
            is_list(snarl_util:ensure_str(E))).

-include("eqc_helper.hrl").
-endif.
-endif.
