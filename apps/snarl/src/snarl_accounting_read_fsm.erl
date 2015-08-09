%% @doc The coordinator for stat get operations.  The key here is to
%% generate the preflist just like in wrtie_fsm and then query each
%% replica and wait until a quorum is met.
-module(snarl_accounting_read_fsm).
-behavior(gen_fsm).
-include("snarl.hrl").

-include("snarl_dtrace.hrl").

%% API
-export([start_link/7, start/3, start/4, start/5]).


-export([reconcile/1, different/1, needs_repair/2, repair/4, unique/1]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2, wait_for_n/2, finalize/2]).

-ignore_xref([
              code_change/4,
              different/1,
              execute/2,
              finalize/2,
              handle_event/3,
              handle_info/3,
              handle_sync_event/4,
              init/1,
              needs_repair/2,
              prepare/2,
              reconcile/1,
              repair/4,
              start/4,
              start_link/7,
              terminate/3,
              unique/1,
              wait_for_n/2,
              waiting/2,
              start/3, start/5
             ]).

-record(state, {raw = false ::boolean(),
                req_id,
                from,
                accounting,
                has_send = false,
                start,
                op,
                r,
                n,
                preflist,
                num_r=0,
                size,
                timeout=?DEFAULT_TIMEOUT,
                val,
                vnode :: atom(),
                bucket,
                system :: atom(),
                replies=[]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, {VNode, System}, Op, From, {Realm, Accounting}, Val, Raw) ->
    gen_fsm:start_link(?MODULE, [ReqID, {VNode, System}, Op, From, {Realm, Accounting}, Val, Raw], []).

start(VNodeInfo, Op, {Realm, Accounting}) ->
    start(VNodeInfo, Op, {Realm, Accounting}, undefined).

start(VNodeInfo, Op, {Realm, Accounting}, Val) ->
    start(VNodeInfo, Op, {Realm, Accounting}, Val, false).

start(VNodeInfo, Op, {Realm, Accounting}, Val, Raw) ->
    ReqID = snarl_vnode:mkid(),
    snarl_accounting_read_fsm_sup:start_read_fsm(
      [ReqID, VNodeInfo, Op, self(), {Realm, Accounting}, Val, Raw]
     ),
    receive
        {ReqID, ok} ->
            ok;
        {ReqID, ok, Result} ->
            {ok, Result}
    after ?DEFAULT_TIMEOUT ->
            {error, timeout}
    end.

%%%===================================================================
%%% States
%%%===================================================================

init([ReqId, {VNode, System}, Op, From, {Realm, Accounting}, Val, Raw]) when
      is_atom(VNode),
      is_atom(System) ->
    ?DT_READ_ENTRY(Accounting, Op),
    SD = #state{raw=Raw,
                bucket=Realm,
                req_id=ReqId,
                from=From,
                op=Op,
                n= ?N,
                r= ?R,
                val=Val,
                start=now(),
                vnode=VNode,
                system=System,
                accounting=Accounting},
    {ok, prepare, SD, 0};

init([ReqId, {VNode, System}, Op, From, {Realm, Accounting}]) when
      is_atom(VNode),
      is_atom(System) ->
    ?DT_READ_ENTRY(Accounting, Op),
    SD = #state{req_id=ReqId,
                bucket=Realm,
                from=From,
                op=Op,
                start=now(),
                vnode=VNode,
                system=System,
                accounting=Accounting},
    {ok, prepare, SD, 0}.

%% @doc Calculate the Preflist.
prepare(timeout, SD0=#state{accounting=Accounting,
                            bucket=Realm,
                            n = N,
                            system=System}) ->
    DocIdx = riak_core_util:chash_key({Realm, Accounting}),
    Prelist = riak_core_apl:get_apl(DocIdx, N, System),
    SD = SD0#state{preflist=Prelist},
    {next_state, execute, SD, 0}.

%% @doc Execute the get reqs.
execute(timeout, SD0=#state{req_id=ReqId,
                            bucket=Realm,
                            accounting=Accounting,
                            op=Op,
                            val=Val,
                            vnode=VNode,
                            preflist=Prelist}) ->
    case Val of
        undefined ->
            VNode:Op(Prelist, ReqId, {Realm, Accounting});
        _ ->
            VNode:Op(Prelist, ReqId, {Realm, Accounting}, Val)
    end,
    {next_state, waiting, SD0}.

%% @doc Wait for R replies and then respond to From (original client
%% that called `snarl:get/2').
%% TODO: read repair...or another blog post?

waiting({ok, ReqID, IdxNode, Obj},
        SD0=#state{from=From, num_r=NumR0, replies=Replies0,
                   r=R, n = N, timeout=Timeout}) ->
    NumR = NumR0 + 1,
    Replies = [{IdxNode, Obj}|Replies0],
    SD = SD0#state{num_r=NumR,replies=Replies},
    if
        NumR >= R ->
            SD1 = case merge(Replies) of
                      not_found ->
                          %% We do not want to return not_found unless we have all
                          %% the replies
                          %% TODO: is this a good idea?
                          SD;
                      Reply ->

                          ?DT_READ_FOUND_RETURN(SD0#state.accounting, SD0#state.op),
                          From ! {ReqID, ok, Reply},
                          SD#state{has_send = true}
                  end,
            if
                %% If we have all repliesbut do not send yet it's not found
                NumR =:= N andalso not SD1#state.has_send ->
                    From ! {ReqID, ok, not_found},
                    {next_state, finalize, SD1#state{has_send = true}, 0};
                %% If we have all replies and also send we're good
                NumR =:= N ->
                    {next_state, finalize, SD1, 0};
                %% If we do not have all replies and not send yet we want
                %% to keep waiting
                not SD1#state.has_send ->
                    {next_state, waiting, SD1};
                %% If we have not all replies but already send the messag
                %% on we've found a object and now can wait for the rest.
                true ->
                    {next_state, wait_for_n, SD1, Timeout}
            end;
        true ->
            {next_state, waiting, SD}
    end.

wait_for_n({ok, _ReqID, IdxNode, Obj},
           SD0=#state{n = N, num_r=NumR, replies=Replies0})
  when NumR =:= N - 1 ->
    Replies = [{IdxNode, Obj}|Replies0],
    {next_state, finalize, SD0#state{num_r=N, replies=Replies}, 0};

wait_for_n({ok, _ReqID, IdxNode, Obj},
           SD0=#state{num_r=NumR0, replies=Replies0, timeout=Timeout}) ->
    NumR = NumR0 + 1,
    Replies = [{IdxNode, Obj}|Replies0],
    {next_state, wait_for_n, SD0#state{num_r=NumR, replies=Replies}, Timeout};

%% TODO partial repair?
wait_for_n(timeout, SD) ->
    {stop, timeout, SD}.

finalize(timeout, SD=#state{
                        vnode=VNode,
                        replies=Replies,
                        bucket=Realm,
                        accounting=Accounting}) ->
    MObj = merge(Replies),
    case needs_repair(MObj, Replies) of
        true ->
            lager:warning("[read] performing read repair on '~p'(~p) <- ~p.",
                          [Accounting, MObj, Replies]),
            repair(VNode, {Realm, Accounting}, MObj, Replies),
            {stop, normal, SD};
        false ->
            {stop, normal, SD}
    end.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @pure
%%
%% @doc Given a list of `Replies' return the merged value.
-spec merge([vnode_reply()]) -> fifo:obj() | not_found.
merge(Replies) ->
    Data = [Obj || {_,Obj} <- Replies],
    Flattened = lists:flatten(Data),
    Unique = lists:usort(Flattened),
    Unique.
    %% TODO: ft_obj:merge(snarl_accounting_read_fsm, Objs).

%% @pure
%%
%% @doc Reconcile conflicts among conflicting values.
-type accounting_list() ::
        [fifo:user()] |
        [fifo:org()] |
        [fifo:role()].
-spec reconcile(accounting_list()) ->
                       fifo:user() | user:role() | fifo:org().

%% TODO:
reconcile(Vs) ->
    Vs.

%% @pure
%%
%% @doc Given the merged object `MObj' and a list of `Replies'
%% determine if repair is needed.
-spec needs_repair(any(), [vnode_reply()]) -> boolean().
needs_repair(MObj, Replies) ->
    Objs = [lists:usort(Obj) || {_,Obj} <- Replies],
    lists:any(different(MObj), Objs).

%% @pure
different(A) -> fun(B) -> (A =/= B) end.

%% @impure
%%
%% @doc Repair any vnodes that do not have the correct object.
-spec repair(atom(), {binary(), binary()}, fifo:obj(), [vnode_reply()]) -> io.
repair(_, _, _, []) -> io;

repair(VNode, StatName, MObj, [{IdxNode,Obj}|T]) ->
    case MObj =:= Obj of
        true ->
            repair(VNode, StatName, MObj, T);
        false ->
            case Obj of
                not_found ->
                    VNode:repair(IdxNode, StatName, MObj);
                _ ->
                    VNode:repair(IdxNode, StatName, MObj)
            end,
            repair(VNode, StatName, MObj, T)
    end.

%% pure
%%
%% @doc Given a list return the set of unique values.
-spec unique([A::any()]) -> [A::any()].
unique(L) ->
    sets:to_list(sets:from_list(L)).
