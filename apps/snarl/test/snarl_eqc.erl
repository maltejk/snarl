-module(snarl_eqc).

-ifdef(TEST).
-ifdef(EQC).

-include_lib("fqc/include/fqc.hrl").
-compile(export_all).

str() ->
    ?SUCHTHAT(L, list(choose($a, $z)), L =/= []).

atom() ->
    elements([a,b,c,undefined]).

prop_ensure_str() ->
    ?FORALL(E, oneof([binary(), int(), real(), atom(), str()]),
            is_list(snarl_util:ensure_str(E))).

-endif.
-endif.
