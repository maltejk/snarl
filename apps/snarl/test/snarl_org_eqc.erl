-module(snarl_org_eqc).

-ifdef(TEST).
-ifdef(EQC).

-define(O, snarl_org).
-define(M, ?MODULE).


-define(FWD(C),
        C({_, UUID}) ->
               ?O:C(UUID)).

-define(FWD2(C),
        C({_, UUID}, A1) ->
               ?O:C(UUID, A1)).

-define(FWD3(C),
        C({_, UUID}, A1, A2) ->
               ?O:C(UUID, A1, A2)).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("snarl/include/snarl.hrl").

-compile(export_all).
-import(snarl_test_helper,
        [id/0, permission/0, non_blank_string/0, maybe_oneof/1, maybe_oneof/2,
         lower_char/0, cleanup_mock_servers/0, mock_vnode/2,
         start_mock_servers/0, metadata_value/0, metadata_kvs/0]).

-record(state, {added = [], next_uuid=uuid:uuid4s(), metadata=[]}).

maybe_a_uuid(#state{added = Added}) ->
    ?SUCHTHAT(
       U,
       ?LET(E, ?SUCHTHAT(N,
                         non_blank_string(),
                         lists:keyfind(N, 1, Added) == false),
            oneof([{E, non_blank_string()} | Added])),
       U /= duplicate).

initial_state() ->
    random:seed(now()),
    #state{}.

prop_compare_to_model() ->
    ?FORALL(Cmds,commands(?MODULE),
            begin
                {H,S,Res} = run_commands(?MODULE,Cmds),
                cleanup(),
                ?WHENFAIL(
                   io:format(user, "History: ~p\nState: ~p\nRes: ~p\n", [H,S,Res]),
                   Res == ok)
            end).

cleanup() ->
    catch eqc_vnode ! delete,
    ok.

command(S) ->
    oneof([
           {call, ?M, add, [S#state.next_uuid, non_blank_string()]},
           {call, ?M, delete, [maybe_a_uuid(S)]},
           {call, ?M, wipe, [maybe_a_uuid(S)]},
           {call, ?M, get, [maybe_a_uuid(S)]},
           {call, ?M, get_, [maybe_a_uuid(S)]},
           {call, ?M, lookup, [maybe_a_uuid(S)]},
           %{call, ?M, lookup_, [maybe_a_uuid(S)]},
           {call, ?M, raw, [maybe_a_uuid(S)]},

           %% List
           {call, ?O, list, []},
           {call, ?O, list, [[], bool()]},
           {call, ?O, list_, []},

           %% Metadata
           {call, ?M, set, [maybe_a_uuid(S), metadata_kvs()]},
           {call, ?M, set, [maybe_a_uuid(S), non_blank_string(), metadata_value()]}

          ]).

%% Normal auth takes a name.
auth({_, N}, P) ->
    ?O:auth(N, P, <<>>).

add(UUID, Org) ->
    meck:new(uuid, [passthrough]),
    meck:expect(uuid, uuid4s, fun() -> UUID end),
    R = case ?O:add(Org) of
            duplicate ->
                duplicate;
            {ok, UUID} ->
                {Org, UUID}
        end,
    meck:unload(uuid),
    R.

?FWD(get).
?FWD(get_).
?FWD(raw).

lookup({N, _}) ->
    ?O:lookup(N).

lookup_({N, _}) ->
    ?O:lookup_(N).

?FWD(delete).
?FWD(wipe).

?FWD2(set).
?FWD3(set).


 next_state(S, duplicate, {call, _, add, [_, _Org]}) ->
    S#state{next_uuid=uuid:uuid4s()};

next_state(S = #state{added = Added}, V, {call, _, add, [_, _Org]}) ->
    S#state{added = [V | Added], next_uuid=uuid:uuid4s()};

next_state(S, _V, {call, _, delete, [UUIDAndName]}) ->
    S#state{added = lists:delete(UUIDAndName,  S#state.added)};

next_state(S, _V, {call, _, wipe, [UUIDAndName]}) ->
    S#state{added = lists:delete(UUIDAndName,  S#state.added)};

next_state(S = #state{metadata=Ms}, _V,
           {call, _, set, [{_, UUID}, K, delete]}) ->
    case has_uuid(S, UUID) of
        false ->
            S;
        true ->
            case lists:keyfind(UUID, 1, Ms) of
                {UUID, M} ->
                    Ms1 = proplists:delete(UUID, Ms),
                    S#state{metadata = [{UUID, orddict:erase(K, M)} | Ms1]};
                _  ->
                    S
            end
    end;

next_state(S = #state{metadata=Ms}, _V,
           {call, _, set, [{_, UUID}, K, V]}) ->
    case has_uuid(S, UUID) of
        false ->
            S;
        true ->
            case lists:keyfind(UUID, 1, Ms) of
                {UUID, M} ->
                    Ms1 = proplists:delete(UUID, Ms),
                    S#state{metadata = [{UUID, orddict:store(K, V, M)} | Ms1]};
                _  ->
                    S#state{metadata = [{UUID, [{K, V}]} | Ms]}
            end
    end;

next_state(S, R,
           {call, Mod, set, [UU, KVs]}) ->
    lists:foldl(fun({K, V}, SAcc) ->
                       next_state(SAcc, R, {call, Mod, set, [UU, K, V]})
               end, S, KVs);

next_state(S, _V, _C) ->
    S.

dynamic_precondition(S, {call,snarl_org_eqc, lookup, [{Name, UUID}]}) ->
    case lists:keyfind(Name, 1, S#state.added) of
        false ->
            true;
        {Name, UUID} ->
            true;
        {Name, _} ->
            false
    end;

dynamic_precondition(_S, {call, _, _, Args}) ->
    not lists:member(duplicate, Args);

dynamic_precondition(_, _) ->
    true.

precondition(_S, _) ->
    true.

%% Metadata
postcondition(S, {call, _, set, [{_, UUID}, _, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, set, [{_, UUID}, _, _]}, ok) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, set, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, set, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

%% List

postcondition(#state{added = A}, {call, _, list, []}, {ok, R}) ->
    lists:usort([U || {_, U} <- A]) == lists:usort(R);

postcondition(#state{added = A}, {call, _, list, [_, true]}, {ok, R}) ->
    lists:usort([U || {_, U} <- A]) == lists:usort([UUID || {_, {UUID, _}} <- R]);

postcondition(#state{added = A}, {call, _, list, [_, false]}, {ok, R}) ->
    lists:usort([U || {_, U} <- A]) == lists:usort([UUID || {_, UUID} <- R]);

postcondition(#state{added = A}, {call, _, list_, []}, {ok, R}) ->
    lists:usort([U || {_, U} <- A]) ==
        lists:usort([UUID || {UUID, _} <- R]);


%% General
postcondition(S, {call, _, get, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, get, [{_, UUID}]}, {ok, _U}) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, lookup, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, lookup, [{_, UUID}]}, {ok, Result}) ->
    {ok, UUID} == jsxd:get(<<"uuid">>, Result) andalso has_uuid(S, UUID);

postcondition(S, {call, _, get_, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, get_, [{_, UUID}]}, {ok, U}) ->
    has_uuid(S, UUID) andalso metadata_match(S, UUID, U);

postcondition(S, {call, _, raw, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, raw, [{_, UUID}]}, {ok, _}) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, add, [_UUID, Org]}, duplicate) ->
    has_org(S, Org);

postcondition(#state{added=_Us}, {call, _, add, [_Org, _]}, {error, _}) ->
    false;

postcondition(#state{added=_Us}, {call, _, add, [_Org, _]}, _) ->
    true;

postcondition(#state{added=_Us}, {call, _, delete, [{_, _UUID}]}, ok) ->
    true;

postcondition(#state{added=_Us}, {call, _, wipe, [{_, _UUID}]}, {ok, _}) ->
    true;

postcondition(_S, C, R) ->
    io:format(user, "postcondition(_, ~p, ~p).~n", [C, R]),
    false.

metadata_match(S, UUID, U) ->
    Ks = snarl_org_state:metadata(U),
    Ks == known_metadata(S, UUID).

known_metadata(#state{metadata=Ms}, UUID) ->
    case lists:keyfind(UUID, 1, Ms) of
        {UUID, M} ->
            M;
        _ ->
            []
    end.
has_uuid(#state{added = A}, UUID) ->
    case lists:keyfind(UUID, 2, A) of
        {_, UUID} ->
            true;
        _ ->
            false
    end.

has_org(#state{added = A}, Org) ->
    case lists:keyfind(Org, 1, A) of
        {Org, _} ->
            true;
        _ ->
            false
    end.


%% We kind of have to start a lot of services for this tests :(
setup() ->
    start_mock_servers(),
    mock_vnode(snarl_org_vnode, [0]),

    meck:new(snarl_role, [passthrough]),
    meck:expect(snarl_role, revoke_prefix, fun(_, _) -> ok end),
    meck:expect(snarl_role, list, fun() -> {ok, []} end),
    meck:expect(snarl_role, get_, fun(_) -> {ok, dummy} end),

    meck:new(snarl_opt, [passthrough]),
    meck:expect(snarl_opt, get, fun(_,_,_,_,D) -> D end),
    meck:expect(snarl_opt, set, fun(_,_) ->ok end),

    ok.

cleanup(_) ->
    cleanup_mock_servers(),
    meck:unload(snarl_opt),
    meck:unload(snarl_role),
    ok.

-define(EQC_SETUP, true).

%-define(EQC_NUM_TESTS, 5000).
-define(EQC_EUNIT_TIMEUT, 1200).
-include("eqc_helper.hrl").
-endif.
-endif.
