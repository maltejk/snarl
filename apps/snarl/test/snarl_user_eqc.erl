-module(snarl_user_eqc).

-ifdef(TEST).
-ifdef(EQC).

-define(U, snarl_user).
-define(M, ?MODULE).
-define(REALM, <<"realm">>).


-define(FWD(C),
        C({_, UUID}) ->
               ?U:C(?REALM, UUID)).

-define(FWD2(C),
        C({_, UUID}, A1) ->
               ?U:C(?REALM, UUID, A1)).

-define(FWD3(C),
        C({_, UUID}, A1, A2) ->
               ?U:C(?REALM, UUID, A1, A2)).

-define(EQC_SETUP, true).
-define(EQC_EUNIT_TIMEUT, 1200).

-define(KEY, <<"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDZyw2HsD2TBPpBcCJLge4Eu1N9IXHx0S9APSdC4GEre3h4huNT9LUA78oOB1LDIyqmwbHy5yqVVBht4awmcveaSsBIDEPBrU+ZrSeibg3ikQxBYA+7IG8gwvEqxI9EdbnF6eqstfiUIaLsLuUY2E2b2DGIohy/NIw0tccchLR0kHUGz4yjmMZg78X9ux2VqFhlTfj3xDsagxFjo90FQkrO32SLULFS9fG5Ki8vsvhfkhhtgct74i894lj4DRThqmvgygODXcyvi/wtixaqKqcn+Y1JCr5AsvXvYmWQzdRh9Rv77j0mleo0xqosqXIH1HqsM4CJmdYGCPU7JB6k0j/H testkey@testbox">>).

-import(snarl_test_helper,
        [id/0, permission/0, maybe_oneof/1, cleanup_mock_servers/0,
         mock_vnode/2, start_mock_servers/0, metadata_value/0, metadata_kvs/0,
         handoff/0, handon/1, delete/0]).

-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("fqc/include/fqc.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("snarl/include/snarl.hrl").

-compile(export_all).

-record(state, {added = [], next_uuid=uuid:uuid4s(),
                passwords = [], roles = [], orgs = [],
                yubikeys = [], keys = [], metadata = [],
                pid}).

maybe_a_uuid(#state{added = Added}) ->
    ?SUCHTHAT(
       U,
       ?LET(E, ?SUCHTHAT(N,
                         non_blank_string(),
                         lists:keyfind(N, 1, Added) == false),
            oneof([{E, non_blank_string()} | Added])),
       U /= duplicate).

maybe_a_password(#state{passwords = Pws}) ->
    maybe_oneof(Pws).

initial_state() ->
    catch ets:delete(s2i),
    ets:new(s2i, [named_table, public, ordered_set]),
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

yubikey() ->
    ?LET(L, resize(44, list(lower_char())), list_to_binary(L)).

maybe_role(#state{roles = Rs}) ->
    maybe_oneof([R || {_, R} <- Rs]).

maybe_org(#state{orgs = Os}) ->
    maybe_oneof([O || {_, O} <- Os]).

maybe_yubikey(#state{yubikeys = Ks}) ->
    maybe_oneof([K || {_, K} <- Ks]).

maybe_key(#state{keys = K0}) ->
    ?LET(Ks, K0, maybe_oneof([Ka || {_, Ka} <- lists:flatten([K || {_, K} <- Ks])])).

maybe_keyid(#state{keys = K0}) ->
    ?LET(Ks, K0, maybe_oneof([Ki || {Ki, _} <- lists:flatten([K || {_, K} <- Ks])])).

cleanup() ->
    delete().

command(S) ->
    oneof([
           {call, ?M, add, [S#state.next_uuid, non_blank_string()]},
           {call, ?M, delete, [maybe_a_uuid(S)]},
           {call, ?M, wipe, [maybe_a_uuid(S)]},
           {call, ?M, get, [maybe_a_uuid(S)]},
           {call, ?M, lookup, [maybe_a_uuid(S)]},
           {call, ?M, lookup_, [maybe_a_uuid(S)]},
           {call, ?M, raw, [maybe_a_uuid(S)]},
           {call, ?M, passwd, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?M, auth, [maybe_a_uuid(S), maybe_a_password(S)]},
           {call, ?M, auth, [maybe_a_uuid(S), maybe_a_password(S), basic]},

           %% List
           {call, ?U, list, [?REALM]},
           {call, ?U, list, [?REALM, [], bool()]},
           {call, ?U, list_, [?REALM]},

           %% Permissions
           {call, ?M, grant, [maybe_a_uuid(S), permission()]},
           {call, ?M, revoke, [maybe_a_uuid(S), permission()]},

           %% Role related commands
           {call, ?M, join, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?M, leave, [maybe_a_uuid(S), maybe_role(S)]},

           %% %% Org related commands
           {call, ?M, join_org, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?M, leave_org, [maybe_a_uuid(S), maybe_org(S)]},
           {call, ?M, active, [maybe_a_uuid(S)]},
           {call, ?M, orgs, [maybe_a_uuid(S)]},
           {call, ?M, select_org, [maybe_a_uuid(S), maybe_org(S)]},

           %% SSH key related commands
           {call, ?M, add_key, [maybe_a_uuid(S), non_blank_string(), ?KEY]},
           {call, ?M, revoke_key, [maybe_a_uuid(S), maybe_key(S)]},
           {call, ?M, keys, [maybe_a_uuid(S)]},
           {call, ?U, find_key, [?REALM, maybe_keyid(S)]},

           %% Yubi key related commands
           {call, ?M, add_yubikey, [maybe_a_uuid(S), yubikey()]},
           {call, ?M, remove_yubikey, [maybe_a_uuid(S), maybe_yubikey(S)]},
           {call, ?M, yubikeys, [maybe_a_uuid(S)]},

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
    ?U:auth(?REALM, N, P, <<>>).

%% BASIC auth takes the uuid.
?FWD3(auth).

add(UUID, User) ->
    meck:new(uuid, [passthrough]),
    meck:expect(uuid, uuid4s, fun() -> UUID end),
    R = case ?U:add(?REALM, User) of
            duplicate ->
                duplicate;
            {ok, UUID} ->
                {User, UUID}
        end,
    meck:unload(uuid),
    R.

?FWD(get).
?FWD(raw).

lookup({N, _}) ->
    ?U:lookup(?REALM, N).

lookup_({N, _}) ->
    ?U:lookup_(?REALM, N).

?FWD2(grant).
?FWD2(revoke).
?FWD2(passwd).
?FWD(delete).
?FWD(wipe).

?FWD2(join).
?FWD2(leave).

?FWD2(join_org).
?FWD2(leave_org).
?FWD2(select_org).
?FWD(orgs).
?FWD(active).

?FWD3(add_key).
?FWD2(revoke_key).
?FWD(keys).
?FWD(find_key).

?FWD2(add_yubikey).
?FWD2(remove_yubikey).
?FWD(yubikeys).

?FWD2(set_metadata).

next_state(S = #state{roles=Rs}, _V,
           {call, _, join, [{_, UUID}, Role]}) ->
    case has_uuid(S, UUID) of
        false ->
            S;
        true ->
            case lists:keyfind(UUID, 1, Rs) of
                {UUID, Roles} ->
                    Rs1 = proplists:delete(UUID, Rs),
                    S#state{roles = [{UUID, [Role | Roles]} | Rs1]};
                _  ->
                    S#state{roles = [{UUID, [Role]} | Rs]}
            end
    end;

next_state(S = #state{orgs=Os}, _V,
           {call, _, join_org, [{_, UUID}, Org]}) ->
    case has_uuid(S, UUID) of
        false ->
            S;
        true ->
            case lists:keyfind(UUID, 1, Os) of
                {UUID, Orgs} ->
                    Os1 = proplists:delete(UUID, Os),
                    S#state{orgs = [{UUID, [Org | Orgs]} | Os1]};
                _  ->
                    S#state{orgs = [{UUID, [Org]} | Os]}
            end
    end;


next_state(S = #state{yubikeys=Ks}, _V,
           {call, _, add_yubikey, [{_, UUID}, Key]}) ->
    ID = snarl_yubico:id(Key),
    case has_uuid(S, UUID) of
        false ->
            S;
        true ->
            case lists:keyfind(UUID, 1, Ks) of
                {UUID, Keys} ->
                    Ks1 = proplists:delete(UUID, Ks),
                    S#state{yubikeys = [{UUID, [ID | Keys]} | Ks1]};
                _  ->
                    S#state{yubikeys = [{UUID, [ID]} | Ks]}
            end
    end;

next_state(S = #state{keys=Ks}, _V,
           {call, _, add_key, [{_, UUID}, ID, _]}) ->
    case has_uuid(S, UUID) of
        false ->
            S;
        true ->
            case lists:keyfind(UUID, 1, Ks) of
                {UUID, Keys} ->
                    Ks1 = proplists:delete(UUID, Ks),
                    S#state{keys = [{UUID, lists:usort([ID | Keys])} | Ks1]};
                _  ->
                    S#state{keys = [{UUID, [ID]} | Ks]}
            end
    end;

 next_state(S, duplicate, {call, _, add, [_, _User]}) ->
    S#state{next_uuid=uuid:uuid4s()};

next_state(S = #state{added = Added}, V, {call, _, add, [_, _User]}) ->
    S#state{added = [V | Added], next_uuid=uuid:uuid4s()};

next_state(S, _V, {call, _, delete, [UUIDAndName = {_, UUID}]}) ->
    S#state{added = lists:delete(UUIDAndName,  S#state.added),
            passwords = proplists:delete(UUID, S#state.passwords)};

next_state(S, _V, {call, _, wipe, [UUIDAndName = {_, UUID}]}) ->
    S#state{added = lists:delete(UUIDAndName,  S#state.added),
            passwords = proplists:delete(UUID, S#state.passwords)};

next_state(S, ok, {call, _, passwd, [{_, UUID}, Passwd]}) ->
    case has_uuid(S, UUID) of
        true ->
            Pwds1 = proplists:delete(UUID, S#state.passwords),
            S#state{passwords = [{UUID, Passwd} | Pwds1]};
        _ ->
            S
    end;

next_state(S, _, {call, _, set_metadata, [UU, KVs]}) ->
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

dynamic_precondition(S, {call,snarl_user_eqc, lookup_, [{Name, UUID}]}) ->
    dynamic_precondition(S, {call,snarl_user_eqc, lookup, [{Name, UUID}]});

dynamic_precondition(S, {call,snarl_user_eqc, auth, [{Name, UUID}, _]}) ->
    dynamic_precondition(S, {call,snarl_user_eqc, lookup, [{Name, UUID}]});

dynamic_precondition(S, {call,snarl_user_eqc, lookup, [{Name, UUID}]}) ->
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

%% Roles
postcondition(S, {call, _, join, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, join, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, leave, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, leave, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

%% Orgs
postcondition(S, {call, _, join_org, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, join_org, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, leave_org, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, leave_org, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, active, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, active, [{_, UUID}]}, {ok, _}) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, orgs, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, orgs, [{_, UUID}]}, {ok, _}) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, select_org, [{_, UUID}, Org]}, not_found) ->
    not (has_uuid(S, UUID) andalso
         lists:member(Org, known_orgs(S, UUID)));

postcondition(S, {call, _, select_org, [{_, UUID}, Org]}, ok) ->
    has_uuid(S, UUID) andalso
     lists:member(Org, known_orgs(S, UUID));

%% Keys
postcondition(S, {call, _, add_key, [{_, UUID}, _, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, add_key, [{_, UUID}, _, _]}, ok) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, add_key, [{_, _}, _, _]}, {error,duplicate}) ->
    all_keys(S) /= [];

postcondition(S, {call, _, revoke_key, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, revoke_key, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, keys, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, keys, [{_, UUID}]}, {ok, L}) when is_list(L) ->
    has_uuid(S, UUID);

postcondition(#state{added = _Us}, {call, _, keys, [_UUID]}, _) ->
    false;

%% YubiKeys
postcondition(S, {call, _, add_yubikey, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, add_yubikey, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, remove_yubikey, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, remove_yubikey, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, yubikeys, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, yubikeys, [{_, UUID}]}, {ok, L}) when is_list(L) ->
    has_uuid(S, UUID);

postcondition(#state{added = _Us}, {call, _, yubikeys, [_UUID]}, _) ->
    false;

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
    lists:usort([U || {_, U} <- A]) == lists:usort([UUID || {_, {UUID, _}} <- R]);

postcondition(#state{added = A}, {call, _, list, [?REALM, _, false]}, {ok, R}) ->
    lists:usort([U || {_, U} <- A]) == lists:usort([UUID || {_, UUID} <- R]);

postcondition(#state{keys=Ks}, {call, _, find_key, [?REALM, K]}, not_found) ->
    [true || {Ak, _} <- Ks, Ak == K] == [];

postcondition(#state{added = A}, {call, _, list_, [?REALM]}, {ok, R}) ->
    lists:usort([U || {_, U} <- A]) ==
        lists:usort([UUID || {UUID, _} <- R]);


%% General
postcondition(S, {call, _, passwd, [{_, UUID}, _]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, passwd, [{_, UUID}, _]}, ok) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, auth, [{User, UUID}, Passwd]}, not_found) ->
    not has_user(S, User)
        orelse (lists:keyfind(UUID, 1, S#state.passwords) =/= {UUID, Passwd});

postcondition(S, {call, _, auth, [{_, UUID}, Passwd, basic]}, not_found) ->
    not has_uuid(S, UUID)
        orelse (lists:keyfind(UUID, 1, S#state.passwords) =/= {UUID, Passwd});

postcondition(S, {call, _, get, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, get, [{_, UUID}]}, {ok, _U}) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, lookup, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, lookup, [{_, UUID}]}, {ok, Result}) ->
    {ok, UUID} == jsxd:get(<<"uuid">>, Result) andalso has_uuid(S, UUID);

postcondition(S, {call, _, lookup_, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, lookup_, [{_, UUID}]}, {ok, Result}) ->
    UUID == ft_user:uuid(Result) andalso has_uuid(S, UUID);

postcondition(S, {call, _, raw, [{_, UUID}]}, not_found) ->
    not has_uuid(S, UUID);

postcondition(S, {call, _, raw, [{_, UUID}]}, {ok, _}) ->
    has_uuid(S, UUID);

postcondition(S, {call, _, add, [_UUID, User]}, duplicate) ->
    has_user(S, User);

postcondition(#state{added=_Us}, {call, _, add, [_User, _]}, {error, _}) ->
    false;

postcondition(#state{added=_Us}, {call, _, add, [_User, _]}, _) ->
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

roles_match(#state{roles=Rs}, UUID, U) ->
    Known = case lists:keyfind(UUID, 1, Rs) of
                {UUID, Roles} ->
                    lists:usort(Roles);
                _ ->
                    []
            end,
    ft_user:roles(U) == Known.

orgs_match(S, UUID, U) ->
    ft_user:orgs(U) == known_orgs(S, UUID).

yubikeys_match(S, UUID, U) ->
    ft_user:yubikeys(U) == known_yubikeys(S, UUID).

keys_match(S, UUID, U) ->
    Ks = ft_user:keys(U),
    Ks1 = [I || {I, _K} <- Ks],
    Ks1 == known_keys(S, UUID).

metadata_match(S, UUID, U) ->
    Ks = ft_user:metadata(U),
    Ks == known_metadata(S, UUID).

known_metadata(#state{metadata=Ms}, UUID) ->
    case lists:keyfind(UUID, 1, Ms) of
        {UUID, M} ->
            M;
        _ ->
            []
    end.

known_yubikeys(#state{yubikeys=Ks}, UUID) ->
    case lists:keyfind(UUID, 1, Ks) of
        {UUID, Keys} ->
            lists:usort(Keys);
        _ ->
            []
    end.

known_keys(#state{keys=Ks}, UUID) ->
    case lists:keyfind(UUID, 1, Ks) of
        {UUID, Keys} ->
            lists:usort(Keys);
        _ ->
            []
    end.

all_keys(#state{keys=Ks}) ->
    lists:flatten([K || {_, K} <- Ks]).


known_orgs(#state{orgs=Os}, UUID) ->
    case lists:keyfind(UUID, 1, Os) of
        {UUID, Orgs} ->
            lists:usort(Orgs);
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

has_user(#state{added = A}, User) ->
    case lists:keyfind(User, 1, A) of
        {User, _} ->
            true;
        _ ->
            false
    end.


%% We kind of have to start a lot of services for this tests :(
setup() ->
    start_mock_servers(),
    mock_vnode(snarl_user_vnode, [0]),
    meck:new(snarl_role, [passthrough]),
    meck:expect(snarl_role, revoke_prefix, fun(?REALM, _, _) -> ok end),
    meck:expect(snarl_role, list, fun(?REALM) -> {ok, []} end),
    meck:expect(snarl_role, get, fun(?REALM, _) -> {ok, dummy} end),
    meck:new(snarl_org, [passthrough]),
    meck:expect(snarl_org, remove_target, fun(?REALM, _, _) -> ok end),
    meck:expect(snarl_org, list, fun(?REALM) -> {ok, []} end),
    meck:expect(snarl_org, get, fun(?REALM, _) -> {ok, dummy} end),
    meck:new(snarl_opt, [passthrough]),
    meck:expect(snarl_opt, get, fun(_,_,_,_,D) -> D end),

    ok.

cleanup(_) ->
    cleanup_mock_servers(),
    meck:unload(snarl_role),
    meck:unload(snarl_org),
    meck:unload(snarl_opt),
    ok.

-endif.
-endif.
