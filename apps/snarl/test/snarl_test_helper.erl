-module(snarl_test_helper).


-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("snarl/include/snarl.hrl").

-compile(export_all).

id(I) ->
    {I, eqc}.

id() ->
    id(0).

atom() ->
    elements([a,b,c,undefined]).

not_empty(G) ->
    ?SUCHTHAT(X, G, X /= [] andalso X /= <<>>).

non_blank_string() ->
    not_empty(?LET(X,list(lower_char()), list_to_binary(X))).

%% Generate a lower 7-bit ACSII character that should not cause any problems
%% with utf8 conversion.
lower_char() ->
    %%choose(16#20, 16#7f).
    choose($a, $z).

permission() ->
    ?SIZED(Size, permission(Size)).

permission(Size) ->
    ?LAZY(oneof([[oneof([<<"...">>, perm_entry()])] || Size == 0] ++
                    [[perm_entry() | permission(Size -1)] || Size > 0])).

perm_entry() ->
    oneof([<<"_">>, non_blank_string()]).

maybe_oneof(L) ->
    maybe_oneof(L, non_blank_string()).

maybe_oneof(L, T) ->
    ?LET(E, ?SUCHTHAT(E, T, not lists:member(E, L)), oneof([E | L])).

start_mock_servers() ->
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
    os:cmd("mkdir eqc_vnode_data"),
    application:set_env(fifo_db, db_path, "eqc_vnode_data"),
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
    os:cmd("rm -r eqc_vnode_data"),
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
                        ReqID = id(),
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
                        ReqID = id(),
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
                        ReqID = id(),
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
                        ReqID = id(),
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

-endif.
