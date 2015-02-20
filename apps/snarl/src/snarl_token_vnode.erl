-module(snarl_token_vnode).
-behaviour(riak_core_vnode).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
         delete/3,
         add/4
        ]).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([
              start_vnode/1,
              repair/4,
              get/3,
              delete/3,
              add/4
             ]).

-define(TIMEOUT, 43200).

-define(TIMEOUT_LIMIT, 1000).

-define(TIMEOUT_CYCLE, 100).

-record(state, {
          db,
          tokens,
          partition,
          node,
          cnt = 0,
          access_cnt = 0,
          timeout_limit,
          timeout_cycle
         }).
-define(MASTER, snarl_token_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Token, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Token, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Token) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Token},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

add(Preflist, ReqID, Token, {default, User}) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, Token, {expiery(?TIMEOUT), User}},
                                   {fsm, undefined, self()},
                                   ?MASTER);
add(Preflist, ReqID, Token, {Timeout, User}) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, Token, {expiery(Timeout), User}},
                                   {fsm, undefined, self()},
                                   ?MASTER);

add(Preflist, ReqID, Token, User) ->
    add(Preflist, ReqID, Token, {?TIMEOUT, User}).


delete(Preflist, ReqID, Token) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Token},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    process_flag(trap_exit, true),
    {ok, DataDir} = application:get_env(riak_core, platform_data_dir),
    DBDir = DataDir ++ "/token/" ++ integer_to_list(Partition),
    lager:info("[token] DB Dir: ~s", [DBDir]),
    DBRef = bitcask:open(DBDir, [{expiry_time, max_timeout()}, read_write]),
    TimeoutLimit = dflt_env(token_timeout_limit, ?TIMEOUT_LIMIT),
    TimeoutCycle = dflt_env(token_timeout_cycle, ?TIMEOUT_CYCLE),
    {ok, #state{
            db = DBRef,
            tokens = dict:new(),
            partition = Partition,
            node = node(),
            timeout_limit = TimeoutLimit,
            timeout_cycle = TimeoutCycle
           }}.

handle_command({repair, {Realm, Token}, _, Obj}, _Sender,
               #state{tokens=Tokens0, db = DBRef}=State) ->
    lager:warning("repair performed ~p~n", [Obj]),
    bitcask:put(DBRef,
                term_to_binary({Realm, Token}),
                term_to_binary(Obj)),
    Tokens1 = dict:store({Realm, Token}, Obj, Tokens0),
    {noreply, State#state{tokens=Tokens1}};

handle_command({get, ReqID, {Realm, Token}}, _Sender,
               #state{tokens = Tokens0, db = DBRef} = State) ->
    NodeIdx = {State#state.partition, State#state.node},
    T0 = bitcask_time:tstamp(),
    Key = term_to_binary({Realm, Token}),
    {Tokens1, Res} =
        case dict:find({Realm, Token}, Tokens0) of
            error ->
                case bitcask:get(DBRef, Key) of
                    {ok, V} ->
                        Obj = binary_to_term(V),
                        case ft_obj:val(Obj) of
                            {Exp, _V} when Exp > T0->
                                {dict:store({Realm, Token}, Obj, Tokens0), Obj};
                            _ ->
                                bitcask:delete(DBRef, Key),
                                {Tokens0, not_found}
                        end;
                    _ ->
                        {Tokens0, not_found}
                end;
            {ok, Obj} ->
                case ft_obj:val(Obj) of
                    {Exp, _V} when Exp > T0->
                        {Tokens0, Obj};
                    _ ->
                        bitcask:delete(DBRef, Key),
                        {dict:erase({Realm, Token}, Tokens0),
                         not_found}
                end
        end,
    {reply, {ok, ReqID, NodeIdx, Res}, State#state{tokens = Tokens1}};

handle_command({delete, {ReqID, _Coordinator}, {Realm, Token}}, _Sender,
               #state{tokens = Tokens, db = DBRef} = State) ->
    bitcask:delete(DBRef, term_to_binary({Realm, Token})),
    {reply, {ok, ReqID},
     State#state{
       tokens = dict:erase({Realm, Token}, Tokens)
      }};

handle_command({add, {ReqID, Coordinator}, {Realm, Token}, {Exp, Value}},
               _Sender, State) ->
    TObject = ft_obj:new({Exp, Value}, Coordinator),
    State1 = expire(State),
    Ts0 = dict:store({Realm, Token}, TObject, State1#state.tokens),
    bitcask:put(State1#state.db, term_to_binary({Realm, Token}),
                term_to_binary(TObject)),
    {reply, {ok, ReqID, Token}, State1#state{
                                  tokens = Ts0,
                                  access_cnt = State1#state.access_cnt + 1,
                                  cnt = State1#state.cnt + 1
                                 }};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = dict:fold(Fun, Acc0, State#state.tokens),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {{Realm, Token}, HObject} = binary_to_term(Data),
    Hs0 = dict:store({Realm, Token}, {now(), HObject}, State#state.tokens),
    {reply, ok, State#state{tokens = Hs0}}.

encode_handoff_item(Token, {_, Data}) ->
    term_to_binary({Token, Data}).

is_empty(State) ->
    case dict:size(State#state.tokens) of
        0 ->
            {true, State};
        _ ->
            {true, State}
    end.

delete(State) ->
    {ok, State#state{tokens = dict:new()}}.

handle_coverage({list, Realm, ReqID}, _KeySpaces, _Sender, State) ->
    Ks = [T || {R, T} <- dict:fetch_keys(State#state.tokens), R =:= Realm],
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, Ks},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{db=DBRef}) ->
    bitcask:close(DBRef),
    ok.


%%%===================================================================
%%% Internal Functions
%%%===================================================================

dflt_env(N, D) ->
    case application:get_env(N) of
        undefined ->
            D;
        {ok, V} ->
            V
    end.

expire(#state{cnt = Cnt, timeout_limit = Limit} = State)
  when Limit < Cnt ->
    State;
expire(#state{access_cnt = Cnt, timeout_cycle = Cycle} = State)
  when Cycle < Cnt ->
    State;

expire(#state{tokens = Tokens, db = DBRef} = State) ->
    T0 = bitcask_time:tstamp(),
    Tokens1 = dict:filter(
                fun({Realm, Token}, Obj) ->
                        case ft_obj:val(Obj) of
                            {Exp, _} when T0 =< Exp ->
                                lager:debug("[token:~s] Expiering: ~s", [Realm, Token]),
                                Key = term_to_binary({Realm, Token}),
                                bitcask:delete(DBRef, Key),
                                false;
                            _ ->
                                true
                        end
                end, Tokens),
    State#state{
      access_cnt = 0,
      tokens = Tokens1,
      cnt = dict:size(Tokens1)
     };

expire(State) ->
    State.

expiery(T) ->
    bitcask_time:tstamp() + T.

max_timeout() ->
    max_timeout(dflt_env(token_timeout, ?TIMEOUT)).

max_timeout(T) ->
    SubSys = [code_grant, refresh_token, client_credentials,
              password_credentials, expiry_time],
    max_timeout(SubSys, T).

max_timeout([S | R], T) ->
    case oauth2_config:expiry_time(S) of
        T0 when T0 > T ->
            max_timeout(R, T0);
        _ ->
            max_timeout(R, T)
    end;

max_timeout([], T) ->
    T.
