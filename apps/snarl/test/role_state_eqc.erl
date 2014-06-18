-module(role_state_eqc).

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile(export_all).

-define(R, snarl_role_state).
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

role() ->
    ?SIZED(Size, role(Size)).

role(Size) ->
    ?LAZY(oneof([{call, ?R, new, [id(Size)]} || Size == 0] ++
                    [?LETSHRINK(
                        [R], [role(Size - 1)],
                        oneof([
                               {call, ?R, load, [id(Size), R]},
                               {call, ?R, uuid, [id(Size), bin_str(), R]},
                               {call, ?R, name, [id(Size), bin_str(), R]},
                               {call, ?R, grant, [id(Size), permission(), R]},
                               {call, ?R, revoke, [id(Size), maybe_oneof(calc_perms(R)), R]}
                              ]))
                     || Size > 0])).

calc_perms({call, _, grant, [_, P, R]}) ->
    [P | calc_perms(R)];
calc_perms({call, _, revoke, [_, P, R]}) ->
    lists:delete(P, lists:usort(calc_perms(R)));
calc_perms({call, _, _, R}) ->
    calc_perms(lists:last(R));
calc_perms(_) ->
    [].

r(K, V, U) ->
    lists:keystore(K, 1, U, {K, V}).

model_uuid(N, R) ->
    r(<<"uuid">>, N, R).

model_name(N, R) ->
    r(<<"name">>, N, R).

model_revoke(P, R) ->
    r(<<"permissions">>, lists:delete(P, permissions(R)), R).

model_grant(P, R) ->
    r(<<"permissions">>, lists:usort([P | permissions(R)]), R).

model(R) ->
    ?R:to_json(R).

permissions(U) ->
    {<<"permissions">>, Ps} = lists:keyfind(<<"permissions">>, 1, U),
    Ps.


prop_name() ->
    ?FORALL({N, R},
            {bin_str(), role()},
            begin
                Role = eval(R),
                ?WHENFAIL(io:format(user, "History: ~p~nRole: ~p~n", [R,Role]),
                          model(?R:name(id(?BIG_TIME), N, Role)) ==
                              model_name(N, model(Role)))
            end).

prop_uuid() ->
    ?FORALL({N, R},
            {bin_str(), role()},
            begin
                Role = eval(R),
                ?WHENFAIL(io:format(user, "History: ~p~nRole: ~p~n", [R, Role]),
                          model(?R:uuid(id(?BIG_TIME), N, Role)) ==
                              model_uuid(N, model(Role)))
            end).

prop_grant() ->
    ?FORALL({P, R},
            {permission(), role()},
            begin
                Role = eval(R),
                R1 = ?R:grant(id(?BIG_TIME), P, Role),
                M1 = model_grant(P, model(Role)),
                ?WHENFAIL(io:format(user, "History: ~p~nRole: ~p~nModel: ~p~n"
                                    "Role: ~p~nModel: ~p~n", [R, Role, model(Role), R1, M1]),
                          model(R1) == M1)
            end).

prop_revoke() ->
    ?FORALL({R, P}, ?LET(R, role(), {R, maybe_oneof(calc_perms(R))}),
            begin
                Role = eval(R),
                ?WHENFAIL(io:format(user, "History: ~p~nUser: ~p~nModel: ~p~n", [R, Role, model(Role)]),
                          model(?R:revoke(id(?BIG_TIME), P, Role)) ==
                              model_revoke(P, model(Role)))
            end).


-include("eqc_helper.hrl").
-endif.
-endif.
