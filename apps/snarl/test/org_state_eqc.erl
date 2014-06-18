-module(org_state_eqc).

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile(export_all).

-define(O, snarl_org_state).
%% This is larger then and time we ever get in the size, used for ensure setting data
%% in LWW registers.
-define(BIG_TIME, 1000000000).

id(T) ->
    {T, eqc}.

bin_str() ->
    ?LET(S, ?SUCHTHAT(L, list(choose($a, $z)), L =/= []), list_to_binary(S)).

permission() ->
    ?SIZED(Size, permission(Size)).

permission(Size) ->
    ?LAZY(oneof([[oneof([<<"...">>, perm_entry()])] || Size == 0] ++
                    [[perm_entry() | permission(Size -1)] || Size > 0])).

perm_entry() ->
    oneof([<<"_">>, bin_str()]).

maybe_oneof(L) ->
    ?LET(E, ?SUCHTHAT(E, bin_str(), not lists:member(E, L)),
         oneof([E | L])).

org() ->
    ?SIZED(Size, org(Size)).

org(Size) ->
    ?LAZY(oneof([{call, ?O, new, [id(Size)]} || Size == 0] ++
                    [?LETSHRINK(
                        [O], [org(Size - 1)],
                        oneof([
                               {call, ?O, load, [id(Size), O]},
                               {call, ?O, uuid, [id(Size), bin_str(), O]},
                               {call, ?O, name, [id(Size), bin_str(), O]},
                               {call, ?O, set_metadata, [id(Size), bin_str(), bin_str(), O]},
                               {call, ?O, set_metadata, [id(Size), maybe_oneof(calc_metadata(O)), delete, O]}

                              ]))
                     || Size > 0])).

calc_metadata({call, _, set_metadata, [_, delete, K, U]}) ->
    lists:delete(K, lists:usort(calc_metadata(U)));
calc_metadata({call, _, set_metadata, [_, I, _K, U]}) ->
    [I | calc_metadata(U)];
calc_metadata({call, _, _, P}) ->
    calc_metadata(lists:last(P));
calc_metadata(_) ->
    [].

r(K, V, U) ->
    lists:keystore(K, 1, U, {K, V}).

model_uuid(N, R) ->
    r(<<"uuid">>, N, R).

model_name(N, R) ->
    r(<<"name">>, N, R).

model_set_metadata(K, V, U) ->
    r(<<"metadata">>, lists:usort(r(K, V, metadata(U))), U).

model_delete_metadata(K, U) ->
    r(<<"metadata">>, lists:keydelete(K, 1, metadata(U)), U).

model(R) ->
    ?O:to_json(R).

metadata(U) ->
    {<<"metadata">>, M} = lists:keyfind(<<"metadata">>, 1, U),
    M.

prop_name() ->
    ?FORALL({N, R},
            {bin_str(), org()},
            begin
                Org = eval(R),
                ?WHENFAIL(io:format(user, "History: ~p~nOrg: ~p~n", [R,Org]),
                          model(?O:name(id(?BIG_TIME), N, Org)) ==
                              model_name(N, model(Org)))
            end).

prop_uuid() ->
    ?FORALL({N, R},
            {bin_str(), org()},
            begin
                Org = eval(R),
                ?WHENFAIL(io:format(user, "History: ~p~nOrg: ~p~n", [R, Org]),
                          model(?O:uuid(id(?BIG_TIME), N, Org)) ==
                              model_uuid(N, model(Org)))
            end).

prop_set_metadata() ->
    ?FORALL({K, V, O}, {bin_str(), bin_str(), org()},
            begin
                Org = eval(O),
                O1 = ?O:set_metadata(id(?BIG_TIME), K, V, Org),
                M1 = model_set_metadata(K, V, model(Org)),
                ?WHENFAIL(io:format(org, "History: ~p~nOrg: ~p~nModel: ~p~n"
                                    "Org': ~p~nModel': ~p~n", [O, Org, model(Org), O1, M1]),
                          model(O1) == M1)
            end).

prop_remove_metadata() ->
    ?FORALL({O, K}, ?LET(O, org(), {O, maybe_oneof(calc_metadata(O))}),
            begin
                Org = eval(O),
                O1 = ?O:set_metadata(id(?BIG_TIME), K, delete, Org),
                M1 = model_delete_metadata(K, model(Org)),
                ?WHENFAIL(io:format(org, "History: ~p~nOrg: ~p~nModel: ~p~n"
                                    "Org': ~p~nModel': ~p~n", [O, Org, model(Org), O1, M1]),
                          model(O1) == M1)
            end).

-include("eqc_helper.hrl").
-endif.
-endif.
