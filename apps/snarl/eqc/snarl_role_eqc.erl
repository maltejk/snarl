-module(snarl_role_eqc).

-ifdef(TEST).
-ifdef(EQC).

-define(RO, snarl_role).
-define(M, ?MODULE).

-define(REALM, <<"realm">>).

-define(FWD(C),
        C({_, UUID}) ->
               ?RO:C(?REALM, UUID)).

-define(FWD2(C),
        C({_, UUID}, A1) ->
               ?RO:C(?REALM, UUID, A1)).

-define(FWD3(C),
        C({_, UUID}, A1, A2) ->
               ?RO:C(?REALM, UUID, A1, A2)).


%-define(EQC_NUM_TESTS, 5000).
-define(EQC_EUNIT_TIMEUT, 1200).

-import(snarl_test_helper,
        [id/0, permission/0, maybe_oneof/1, cleanup_mock_servers/0,
         mock_vnode/2, start_mock_servers/0, metadata_value/0, metadata_kvs/0,
         handoff/0, handon/1, delete/0]).

-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("fqc/include/fqci.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-compile(export_all).

-record(state, {added = [], next_uuid=fifo_utils:uuid(), metadata = []}).

maybe_a_uuid(#state{added = Added}) ->
    ?SUCHTHAT(
       U,
       ?LET(E, ?SUCHTHAT(N,
                         non_blank_string(),
                         lists:keyfind(N, 1, Added) == false),
            oneof([{E, non_blank_string()} | Added])),
       U /= duplicate).

initial_state() ->
    random:seed(erlang:timestamp()),
    #state{}.

prop_compare_role_to_model() ->
    ?SETUP(fun setup/0,
           ?FORALL(Cmds,commands(?MODULE),
                   begin
                       {H,S,Res} = run_commands(?MODULE,Cmds),
                       cleanup(),
                       ?WHENFAIL(
                          io:format(user, "History: ~p\nState: ~p\nRes: ~p\n", [H,S,Res]),
                          Res == ok)
                   end)).

cleanup() ->
    delete().

command(S) ->
    oneof([
           {call, ?M, add, [S#state.next_uuid, non_blank_string()]},
           {call, ?M, delete, [maybe_a_uuid(S)]},
           {call, ?M, wipe, [maybe_a_uuid(S)]},
           {call, ?M, get, [maybe_a_uuid(S)]},
           {call, ?M, lookup, [maybe_a_uuid(S)]},
           {call, ?M, raw, [maybe_a_uuid(S)]},

           %% List
           {call, ?RO, list, [?REALM]},
           {call, ?RO, list, [?REALM, [], bool()]},
           %%{call, ?RO, list_, [?REALM]},

           %% Permissions
           {call, ?M, grant, [maybe_a_uuid(S), permission()]},
           {call, ?M, revoke, [maybe_a_uuid(S), permission()]},

           %% Metadata
           {call, ?M, set_metadata, [maybe_a_uuid(S), metadata_kvs()]},

           %% Meta command
           {call, ?M, handoff_handon, []}

          ]).

handoff_handon() ->
    Data = handoff(),
    delete(),
    handon(Data).

%% Normal auth takes a name.
auth({_, N}, P) ->
    ?RO:auth(?REALM, N, P, <<>>).

%% BASIC auth takes the uuid.
?FWD3(auth).

add(UUID, Role) ->
    meck:new(fifo_utils, [passthrough]),
    meck:expect(fifo_utils, uuid, fun(role) -> UUID end),
    R = case ?RO:add(?REALM, Role) of
            duplicate ->
                duplicate;
            {ok, UUID} ->
                {Role, UUID}
        end,
    meck:unload(fifo_utils),
    R.

?FWD(get).
?FWD(raw).

lookup({N, _}) ->
    ?RO:lookup(?REALM, N).

?FWD2(grant).
?FWD2(revoke).
?FWD(delete).
?FWD(wipe).


?FWD2(set_metadata).

 next_state(S, duplicate, {call, _, add, [_, _Role]}) ->
    S#state{next_uuid=fifo_utils:uuid()};

next_state(S = #state{added = Added}, V, {call, _, add, [_, _Role]}) ->
    S#state{added = [V | Added], next_uuid=fifo_utils:uuid()};

next_state(S, _V, {call, _, delete, [UUIDAndName]}) ->
    S#state{added = lists:delete(UUIDAndName,  S#state.added)};

next_state(S, _V, {call, _, wipe, [UUIDAndName]}) ->
    S#state{added = lists:delete(UUIDAndName,  S#state.added)};

next_state(S, _R,
           {call, _, set_metadata, [UU, KVs]}) ->
    lists:foldl(fun({K, V}, SAcc) ->
                        do_metadata(SAcc, UU, K, V)
               end, S, KVs);

next_state(S, _V, _C) ->
    S.

do_metadata(S = #state{metadata=Ms}, UUID, K, V) ->
    case has_uuid(S, UUID) of
        false ->
            S;
        true ->
            case lists:keyfind(UUID, 1, Ms) of
                {UUID, M} ->
                    Ms1 = proplists:delete(UUID, Ms),
                    Ms2 = case V of
                              delete ->
                                  [{UUID, orddict:erase(K, M)} | Ms1];
                              _ ->
                                  [{UUID, orddict:store(K, V, M)} | Ms1]
                          end,
                    S#state{metadata = Ms2};
                _  ->
                    case V of
                        delete ->
                            S;
                        _ ->
                            S#state{metadata = [{UUID, [{K, V}]} | Ms]}
                    end
            end
    end.

dynamic_precondition(S, {call,snarl_role_eqc, lookup, [{Name, UUID}]}) ->
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

%% Permissions
postcondition(S, {call, _, grant, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, grant, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, revoke, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, revoke, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

%% Metadata
postcondition(S, {call, _, set_metadata, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, set_metadata, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

%% List

postcondition(#state{added = A}, {call, _, list, [?REALM]}, {ok, R}) ->
    lists:usort([U || {_, U} <- A]) == lists:usort(R);

postcondition(#state{added = A}, {call, _, list, [?REALM, _, true]}, {ok, R}) ->
    lists:usort([U || {_, U} <- A]) == lists:usort([ft_role:uuid(Role) || {_, Role} <- R]);

postcondition(#state{added = A}, {call, _, list, [?REALM, _, false]}, {ok, R}) ->
    lists:usort([U || {_, U} <- A]) == lists:usort([UUID || {_, UUID} <- R]);

postcondition(#state{added = A}, {call, _, list_, [?REALM]}, {ok, R}) ->
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
    UUID == ft_role:uuid(Result) andalso has_uuid(S, UUID);

postcondition(S, {call, _, raw, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, raw, [{_, UUID}]}, {ok, _}) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, add, [_UUID, Role]}, duplicate) ->
    has_role(S, Role);

postcondition(#state{added=_Us}, {call, _, add, [_Role, _]}, {error, _}) ->
    false;

postcondition(#state{added=_Us}, {call, _, add, [_Role, _]}, _) ->
    true;

postcondition(#state{added=_Us}, {call, _, delete, [{_, _UUID}]}, ok) ->
    true;

postcondition(#state{added=_Us}, {call, _, wipe, [{_, _UUID}]}, {ok, _}) ->
    true;

postcondition(_S, {call, _,handoff_handon, []}, _) ->
    true;

postcondition(_S, C, R) ->
    io:format(user, "postcondition(_, ~p, ~p).~n", [C, R]),
    false.

metadata_match(S, UUID, U) ->
    Ks = ft_role:metadata(U),
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

has_role(#state{added = A}, Role) ->
    case lists:keyfind(Role, 1, A) of
        {Role, _} ->
            true;
        _ ->
            false
    end.

%% We kind of have to start a lot of services for this tests :(
setup() ->
    start_mock_servers(),
    mock_vnode(snarl_role_vnode, [0]),
    meck:new(snarl_org, [passthrough]),
    meck:expect(snarl_org, remove_target, fun(?REALM, _, _) -> ok end),
    meck:expect(snarl_org, list, fun(?REALM) -> {ok, []} end),
    meck:expect(snarl_org, get, fun(?REALM, _) -> {ok, dummy} end),
    meck:new(snarl_user, [passthrough]),
    meck:expect(snarl_user, revoke_prefix, fun(?REALM, _, _) -> ok end),
    meck:expect(snarl_user, leave, fun(?REALM, _, _) -> ok end),
    meck:expect(snarl_user, list, fun(?REALM) -> {ok, []} end),
    meck:expect(snarl_user, get, fun(?REALM, _) -> {ok, dummy} end),
    meck:new(snarl_opt, [passthrough]),
    meck:expect(fifo_opt, get, fun(_,_,_,_,_,D) -> D end),
    fun() ->
            cleanup_mock_servers(),
            meck:unload(snarl_org),
            meck:unload(snarl_user),
            meck:unload(fifo_opt),
            ok
    end.

-endif.
-endif.
