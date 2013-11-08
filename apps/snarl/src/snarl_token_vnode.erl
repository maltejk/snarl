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

-define(TIMEOUT, 43200000).

-define(TIMEOUT_LIMIT, 1000).

-define(TIMEOUT_CYCLE, 100).

-record(state, {
          tokens,
          partition,
          node,
          cnt = 0,
          access_cnt = 0,
          timeout,
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

add(Preflist, ReqID, Token, User) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, Token, User},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Token) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Token},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    Timeout = dflt_env(token_timeout, ?TIMEOUT),
    TimeoutLimit = dflt_env(token_timeout_limit, ?TIMEOUT_LIMIT),
    TimeoutCycle = dflt_env(token_timeout_cycle, ?TIMEOUT_CYCLE),
    {ok, #state{
       tokens = dict:new(),
       partition = Partition,
       node = node(),
       timeout = Timeout,
       timeout_limit = TimeoutLimit,
       timeout_cycle = TimeoutCycle
      }}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Token, _, Obj}, _Sender, #state{tokens=Tokens0}=State) ->
    lager:warning("repair performed ~p~n", [Obj]),
    Tokens1 = dict:store(Token, {now(), Obj}, Tokens0),
    {noreply, State#state{tokens=Tokens1}};

handle_command({get, ReqID, Token}, _Sender, #state{tokens = Tokens0} = State) ->
    NodeIdx = {State#state.partition, State#state.node},
    {Tokens1, Res} = case dict:find(Token, Tokens0) of
                         error ->
                             {Tokens0,
                              {ok, ReqID, NodeIdx, not_found}};
                         {ok, {_, V}} ->
                             {dict:update(Token, fun({_, User}) ->
                                                         {now(), User}
                                                 end, Tokens0),
                              {ok, ReqID, NodeIdx, V}}
                     end,
    {reply,
     Res,
     State#state{tokens = Tokens1}};

handle_command({delete, {ReqID, _Coordinator}, Token}, _Sender, State) ->
    {reply, {ok, ReqID},
     State#state{
       tokens = dict:erase(Token, State#state.tokens)
      }};

handle_command({add, {ReqID, Coordinator}, Token, User}, _Sender, State) ->
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    TObject = #snarl_obj{val=User, vclock=VC},
    State1 = expire(State),
    Ts0 = dict:store(Token, {now(), TObject}, State1#state.tokens),

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
    {Token, HObject} = binary_to_term(Data),
    Hs0 = dict:store(Token, {now(), HObject}, State#state.tokens),
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

handle_coverage({list, ReqID}, _KeySpaces, _Sender, State) ->
    {reply,
     {ok, ReqID, {State#state.partition,State#state.node}, dict:fetch_keys(State#state.tokens)},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
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

expire(#state{tokens = Tokens, timeout = Timeout} = State) ->
    Tokens1 = dict:filter(fun(_K, {Timer, _V}) ->
                                  timer:now_diff(Timer, now()) =< Timeout
                          end, Tokens),
    State#state{
      access_cnt = 0,
      tokens = Tokens1,
      cnt = dict:size(Tokens1)
     };
expire(State) ->
    State.
