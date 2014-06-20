-module(snarl_user_eqc).

-ifdef(TEST).
-ifdef(EQC).

-define(U, snarl_user).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("snarl/include/snarl.hrl").

-compile(export_all).
-import(snarl_test_helper,
        [id/0, permission/0, non_blank_string/0, maybe_oneof/1, lower_char/0,
         cleanup_mock_servers/0, mock_vnode/2, start_mock_servers/0]).

-record(state, {added = [], next_uuid=uuid:uuid4s(),
                passwords = [], roles = [], orgs = [],
                yubikeys = []}).


maybe_a_uuid(#state{added = Added}) ->
    maybe_oneof(Added).

initial_state() ->
    random:seed(now()),
    #state{}.

prop_test() ->
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


cleanup() ->
    eqc_vnode ! delete,
    ok.

command(S) ->
    oneof([
           {call, ?U, get, [maybe_a_uuid(S)]},
           {call, ?U, get_, [maybe_a_uuid(S)]},
           {call, ?U, raw, [maybe_a_uuid(S)]},

           {call, ?U, grant, [maybe_a_uuid(S), permission()]},
           {call, ?U, revoke, [maybe_a_uuid(S), permission()]},
           {call, ?U, passwd, [maybe_a_uuid(S), non_blank_string()]},
           %% Role related commands
           {call, ?MODULE, join, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?U, leave, [maybe_a_uuid(S), maybe_role(S)]},
           %% Org related commands
           {call, ?MODULE, join_org, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?U, leave_org, [maybe_a_uuid(S), maybe_org(S)]},
           {call, ?U, active, [maybe_a_uuid(S)]},

           %% SSH key related commands
           {call, ?U, add_key, [maybe_a_uuid(S), non_blank_string(), non_blank_string()]},
           {call, ?U, revoke_key, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?U, keys, [maybe_a_uuid(S)]},

           %% Yubi key related commands
           {call, ?U, add_yubikey, [maybe_a_uuid(S), yubikey()]},
           {call, ?U, remove_yubikey, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?U, yubikeys, [maybe_a_uuid(S)]},

           {call, ?MODULE, add, [S#state.next_uuid, maybe_a_uuid(S)]},
           {call, ?U, delete, [maybe_a_uuid(S)]}
          ]).

add(UUID, User) ->
    case snarl_user_vnode:add(0, id(), UUID, User) of
        {ok, 0} ->
            UUID;
        E ->
            {error, E}
    end.
join(UUID, Role) ->
    meck:new(snarl_role, [passthrough]),
    meck:expect(snarl_role, get_, fun(_) -> {ok, dummy} end),
    R = ?U:join(UUID, Role),
    meck:unload(snarl_role),
    R.

join_org(UUID, Org) ->
    meck:new(snarl_org, [passthrough]),
    meck:expect(snarl_org, get_, fun(_) -> {ok, dummy} end),
    R = ?U:join_org(UUID, Org),
    meck:unload(snarl_org),
    R.

next_state(S = #state{added=Us, roles=Rs}, _V,
           {call, _, join, [UUID, Role]}) ->
    case lists:member(UUID, Us) of
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

next_state(S = #state{added=Us, orgs=Os}, _V,
           {call, _, join_org, [UUID, Org]}) ->
    case lists:member(UUID, Us) of
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

next_state(S = #state{added=Us, yubikeys=Ks}, _V,
           {call, _, add_yubikey, [UUID, Key]}) ->
    ID = snarl_yubico:id(Key),
    case lists:member(UUID, Us) of
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

next_state(S = #state{added = Added}, V, {call, _, add, [_, _User]}) ->
    S#state{added = [V | Added], next_uuid=uuid:uuid4s()};

next_state(S, _V, {call, _, delete, [UUID]}) ->
    S#state{added = lists:delete(UUID,  S#state.added),
            passwords = proplists:delete(UUID, S#state.passwords)};

next_state(S, _V, {call, _, passwd, [UUID, Passwd]}) ->
    case lists:member(UUID, S#state.added) of
        true ->
            Pwds1 = proplists:delete(UUID, S#state.passwords),
            S#state{passwords = [{UUID, Passwd} | Pwds1]};
        _ ->
            S
    end;

next_state(S, _V, _C) ->
    S.

precondition(_S, _) ->
    true.

%% Roles
postcondition(#state{added=Us}, {call, _, join, [UUID, _]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, join, [UUID, _]}, ok) ->
    lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, leave, [UUID, _]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, leave, [UUID, _]}, ok) ->
    lists:member(UUID, Us);

%% Orgs
postcondition(#state{added=Us}, {call, _, join_org, [UUID, _]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, join_org, [UUID, _]}, ok) ->
    lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, leave_org, [UUID, _]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, leave_org, [UUID, _]}, ok) ->
    lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, active, [UUID]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, active, [UUID]}, {ok, _}) ->
    lists:member(UUID, Us);

%% Keys
postcondition(#state{added=Us}, {call, _, add_key, [UUID, _, _]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, add_key, [UUID, _, _]}, ok) ->
    lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, revoke_key, [UUID, _]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, revoke_key, [UUID, _]}, ok) ->
    lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, keys, [UUID]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added = Us}, {call, _, keys, [UUID]}, {ok, L}) when is_list(L) ->
    lists:member(UUID, Us);

postcondition(#state{added = _Us}, {call, _, keys, [_UUID]}, _) ->
    false;

%% YubiKeys
postcondition(#state{added=Us}, {call, _, add_yubikey, [UUID, _]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, add_yubikey, [UUID, _]}, ok) ->
    lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, remove_yubikey, [UUID, _]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, remove_yubikey, [UUID, _]}, ok) ->
    lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, yubikeys, [UUID]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added = Us}, {call, _, yubikeys, [UUID]}, {ok, L}) when is_list(L) ->
    lists:member(UUID, Us);

postcondition(#state{added = _Us}, {call, _, yubikeys, [_UUID]}, _) ->
    false;


%% Permissions
postcondition(#state{added=Us}, {call, _, grant, [UUID, _]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, grant, [UUID, _]}, ok) ->
    lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, revoke, [UUID, _]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, revoke, [UUID, _]}, ok) ->
    lists:member(UUID, Us);

%% General
postcondition(#state{added=Us}, {call, _, passwd, [UUID, _]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, passwd, [UUID, _]}, ok) ->
    lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, get, [UUID]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, get, [UUID]}, {ok, _U}) ->
    lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, get_, [UUID]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(S=#state{added=Us}, {call, _, get_, [UUID]}, {ok, U}) ->
    lists:member(UUID, Us) andalso roles_match(S, UUID, U)
        andalso orgs_match(S, UUID, U) andalso yubikeys_match(S, UUID, U);

postcondition(#state{added=Us}, {call, _, raw, [UUID]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, raw, [UUID]}, {ok, _}) ->
    lists:member(UUID, Us);

postcondition(#state{added=_Us}, {call, _, add, [_User, _]}, {error, _}) ->
    false;

postcondition(#state{added=_Us}, {call, _, add, [_User, _]}, _) ->
    true;

postcondition(#state{added=_Us}, {call, _, delete, [_UUID]}, ok) ->
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
    snarl_user_state:roles(U) == Known.

orgs_match(#state{orgs=Os}, UUID, U) ->
    Known = case lists:keyfind(UUID, 1, Os) of
                {UUID, Orgs} ->
                    lists:usort(Orgs);
                _ ->
                    []
            end,
    snarl_user_state:orgs(U) == Known.

yubikeys_match(S, UUID, U) ->
    snarl_user_state:yubikeys(U) == known_yubikeys(S, UUID).

known_yubikeys(#state{yubikeys=Ks}, UUID) ->
    case lists:keyfind(UUID, 1, Ks) of
        {UUID, Keys} ->
            lists:usort(Keys);
        _ ->
            []
    end.

%% We kind of have to start a lot of services for this tests :(
setup() ->
    start_mock_servers(),
    mock_vnode(snarl_user_vnode, [0]),
    ok.

cleanup(_) ->
    cleanup_mock_servers(),
    ok.



-define(EQC_SETUP, true).

%-define(EQC_NUM_TESTS, 5000).
-define(EQC_EUNIT_TIMEUT, 120).
-include("eqc_helper.hrl").
-endif.
-endif.
