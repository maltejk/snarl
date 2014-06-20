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


-record(state, {added = [], next_uuid=uuid:uuid4s(),
                passwords = []}).

reqid(I) ->
    {I, eqc}.

reqid() ->
    reqid(0).

atom() ->
    elements([a,b,c,undefined]).

not_empty(G) ->
    ?SUCHTHAT(X, G, X /= [] andalso X /= <<>>).

non_blank_string() ->
    ?LET(X,not_empty(list(lower_char())), list_to_binary(X)).

maybe_a_uuid(#state{added = Added}) ->
    oneof([non_blank_string() | Added]).


%% Generate a lower 7-bit ACSII character that should not cause any problems
%% with utf8 conversion.
lower_char() ->
    choose(16#20, 16#7f).


permission() ->
    ?SIZED(Size, permission(Size)).

permission(Size) ->
    ?LAZY(oneof([[oneof([<<"...">>, perm_entry()])] || Size == 0] ++
                    [[perm_entry() | permission(Size -1)] || Size > 0])).

perm_entry() ->
    oneof([<<"_">>, non_blank_string()]).


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
           {call, ?U, leave, [maybe_a_uuid(S), non_blank_string()]},
           %% Org related commands
           {call, ?MODULE, join_org, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?U, leave_org, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?U, active, [maybe_a_uuid(S)]},

           %% SSH key related commands
           {call, ?U, add_key, [maybe_a_uuid(S), non_blank_string(), non_blank_string()]},
           {call, ?U, revoke_key, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?U, keys, [maybe_a_uuid(S)]},

           %% Yubi key related commands
           {call, ?U, add_yubikey, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?U, remove_yubikey, [maybe_a_uuid(S), non_blank_string()]},
           {call, ?U, yubikeys, [maybe_a_uuid(S)]},

           {call, ?MODULE, add, [S#state.next_uuid, maybe_a_uuid(S)]},
           {call, ?U, delete, [maybe_a_uuid(S)]}
          ]).

initialize_vnode() ->
    ok.

add(UUID, User) ->
    case snarl_user_vnode:add(0, reqid(), UUID, User) of
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

postcondition(#state{added=Us}, {call, _, get, [UUID]}, {ok, _}) ->
    lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, get_, [UUID]}, not_found) ->
    not lists:member(UUID, Us);

postcondition(#state{added=Us}, {call, _, get_, [UUID]}, {ok, _}) ->
    lists:member(UUID, Us);

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

%% We kind of have to start a lot of services for this tests :(
setup() ->
    application:load(sasl),
    %%application:set_env(sasl, sasl_error_logger, {file, "snarl_user_eqc.log"}),
    application:set_env(snarl, hash_fun, sha512),
    %%error_logger:tty(false),
    error_logger:logfile({open, "put_fsm_eqc.log"}),

    application:stop(lager),
    application:load(lager),
    application:set_env(lager, handlers,
                        [{lager_file_backend, [{"snarl_user_eqc_lager.log", info, 10485760,"$D0",5}]}]),
    ok = lager:start(),
    start_mock_servers(),
    mock_vnode(snarl_user_vnode, [0]),
    ok.

cleanup(_) ->
    cleanup_mock_servers(),
    ok.

start_mock_servers() ->
    os:cmd("mkdir eqc_user_vnode_data"),
    application:set_env(fifo_db, db_path, "eqc_user_vnode_data"),
    application:set_env(fifo_db, backend, fifo_db_leveldb),
    ok = application:start(hanoidb),
    ok = application:start(bitcask),
    ok = application:start(eleveldb),
    application:start(syntax_tools),
    application:start(compiler),
    application:start(goldrush),
    application:start(lager),
    ok = application:start(fifo_db),
    start_fake_read_fsm(),
    start_fake_write_fsm().

cleanup_mock_servers() ->
    eqc_vnode ! stop,
    stop_fake_read_fsm(),
    stop_fake_write_fsm(),
    application:stop(fifo_db),
    os:cmd("rm -r eqc_user_vnode_data"),
    application:stop(bitcask),
    application:stop(compiler),
    application:stop(eleveldb),
    application:stop(goldrush),
    application:stop(hanoidb),
    application:stop(lager),
    application:stop(syntax_tools).

start_fake_vnode_master(Pid) ->
    meck:new(riak_core_vnode_master, [passthrough]),
    meck:expect(riak_core_vnode_master, command,
                fun(_, C, _, _) ->
                        Ref = make_ref(),
                        Pid ! {command, self(), Ref, C},
                        receive
                            {Ref, Res} ->
                                Res
                        after
                            5000 ->
                                {error, timeout}
                        end
                end).
start_fake_read_fsm() ->
    meck:new(snarl_entity_read_fsm, [passthrough]),
    meck:expect(snarl_entity_read_fsm, start,
                fun({M, _}, C, E) ->
                        ReqID = reqid(),
                        R = M:C(dummy, ReqID, E),
                        Res = case R of
                                  {ok, ReqID, _, #snarl_obj{val=V}} ->
                                      {ok, V};
                                  {ok, ReqID, _, O} ->
                                      O;
                                  E ->
                                      E
                              end,
                        Res
                end),
    meck:expect(snarl_entity_read_fsm, start,
                fun({M, _}, C, E, _, true) ->
                        ReqID = reqid(),
                        R = M:C(dummy, ReqID, E),
                        Res = case R of
                                  {ok, ReqID, _, not_found} ->
                                      not_found;
                                  {ok, ReqID, _, O} ->
                                      {ok, O};
                                  {ok, ReqID, _, O} ->
                                      O;
                                  E ->
                                      E
                              end,
                        Res
                end).

start_fake_write_fsm() ->
    meck:new(snarl_entity_write_fsm, [passthrough]),
    meck:expect(snarl_entity_write_fsm, write,
                fun({M, _}, E, C) ->
                        ReqID = reqid(),
                        R = M:C(dummy, ReqID, E),
                        Res = case R of
                                  {ok, ReqID, _, #snarl_obj{val=V}} ->
                                      {ok, V};
                                  {ok, ReqID, _, O} ->
                                      O;
                                  {ok, _} ->
                                      ok;
                                  E ->
                                      E
                              end,
                        Res
                end),
    meck:expect(snarl_entity_write_fsm, write,
                fun({M, _}, E, C, Val) ->
                        ReqID = reqid(),
                        R = M:C(dummy, ReqID, E, Val),
                        Res = case R of
                                  {ok, ReqID, _, #snarl_obj{val=V}} ->
                                      {ok, V};
                                  {ok, ReqID, _, O} ->
                                      O;
                                  {ok, _} ->
                                      ok;
                                  {ok, _, O} ->
                                      O;
                                  E ->
                                      E
                              end,
                        Res
                end).

stop_fake_vnode_master() ->
    catch meck:unload(riak_core_vnode_master).

stop_fake_read_fsm() ->
    catch meck:unload(snarl_entity_read_fsm).

stop_fake_write_fsm() ->
    meck:unload(snarl_entity_write_fsm).

mock_vnode(Mod, Args) ->
    {ok, S, _} = Mod:init(Args),
    Pid = spawn(?MODULE, mock_vnode_loop, [Mod, S]),
    register(eqc_vnode, Pid),
    start_fake_vnode_master(Pid),
    Pid.

mock_vnode_loop(M, S) ->
    receive
        {command, F, Ref, C} ->
            try M:handle_command(C, F, S) of
                {reply, R, S1} ->
                    F ! {Ref, R},
                    mock_vnode_loop(M, S1);
                {_, _, S1} = E->
                    io:format(user, "VNode command Error: ~p~n", [E]),
                    mock_vnode_loop(M, S1);
                E ->
                    io:format(user, "VNode command Error: ~p~n", [E]),
                    mock_vnode_loop(M, S)
            catch
                E:E1 ->
                    io:format(user, "VNode command(~p) crash: ~p:~p~n", [C, E, E1])

            end;
        {coverage, F, Ref, C} ->
            case M:handle_coverage(C, undefinded, F, S) of
                {reply, R, S1} ->
                    F ! {Ref, R},
                    mock_vnode_loop(M, S1);
                {_, _, S1} ->
                    mock_vnode_loop(M, S1)
            end;
        {info, F, Ref, C} ->
            case M:handle_info(C, undefinded, F, S) of
                {reply, R, S1} ->
                    F ! {Ref, R},
                    mock_vnode_loop(M, S1);
                {_, _, S1} ->
                    mock_vnode_loop(M, S1)
            end;
        stop ->
            stop_fake_vnode_master();
        delete ->
            {ok, S1} = M:delete(S),
            mock_vnode_loop(M, S1)
    end.

-define(EQC_SETUP, true).

                                                %-define(EQC_NUM_TESTS, 5000).
-define(EQC_EUNIT_TIMEUT, 120).
-include("eqc_helper.hrl").
-endif.
-endif.
