%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(snarl_entity_write_fsm).
-behavior(gen_fsm).
-include("snarl.hrl").

%% API
-export([start_link/5, start_link/6, write/3, write/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2]).


-ignore_xref([
              code_change/4,
              execute/2,
              handle_event/3,
              handle_info/3,
              handle_sync_event/4,
              init/1,
              prepare/2,
              start_link/5,
              start_link/6,
              terminate/3,
              waiting/2
             ]).

%% req_id: The request id so the caller can verify the response.
%%
%% from: The pid of the sender so a reply can be made.
%%
%% entity: The entity.
%%
%% op: The stat op, one of [add, delete, grant, revoke].
%%
%% val: Additional arguments passed.
%%
%% prelist: The preflist for the given {Client, StatName} pair.
%%
%% num_w: The number of successful write replies.
-record(state, {req_id :: pos_integer(),
                from :: pid(),
                entity :: string(),
                op :: atom(),
                vnode,
                start,
                system,
                w,
                n,
                cordinator :: node(),
                val = undefined :: term() | undefined,
                preflist :: riak_core_apl:preflist2(),
                num_w = 0 :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link({VNode, System}, ReqID, From, Entity, Op) ->
    start_link({node(), VNode, System}, ReqID, From, Entity, Op);

start_link({Node, VNode, System}, ReqID, From, Entity, Op) ->
    start_link({Node, VNode, System}, ReqID, From, Entity, Op, undefined).

start_link({VNode, System}, ReqID, From, Entity, Op, Val) ->
    start_link({node(), VNode, System}, ReqID, From, Entity, Op, Val);


start_link({Node, VNode, System}, ReqID, From, Entity, Op, Val) ->
    gen_fsm:start_link(?MODULE, [{Node, VNode, System}, ReqID, From, Entity, Op, Val], []).

write({VNode, System}, User, Op) ->
    write({node(), VNode, System}, User, Op);

write({Node, VNode, System}, User, Op) ->
    write({Node, VNode, System}, User, Op, undefined).

write({VNode, System}, User, Op, Val) ->
    write({node(), VNode, System}, User, Op, Val);

write({{remote, Node}, VNode, System}, User, Op, Val) ->
    lager:info("[sync-write:~p] executing sync write", [System]),
    ReqID = snarl_vnode:mk_reqid(),
    snarl_entity_write_fsm_sup:start_write_fsm([{Node, VNode, System}, ReqID, undefined, User, Op, Val]);

write({Node, VNode, System}, User, Op, Val) ->
    ReqID = snarl_vnode:mk_reqid(),
    snarl_entity_write_fsm_sup:start_write_fsm([{Node, VNode, System}, ReqID, self(), User, Op, Val]),
    receive
        {ReqID, ok} ->
            snarl_sync:sync_op(Node, VNode, System, User, Op, Val),
            ok;
        {ReqID, ok, Result} ->
            snarl_sync:sync_op(Node, VNode, System, User, Op, Val),
            {ok, Result}
            %%;
            %%Other ->
            %%lager:error("Unknown write reply: ~p", [Other])
    after ?DEFAULT_TIMEOUT ->
            {error, timeout}
    end.

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.

init([{Node, VNode, System}, ReqID, From, Entity, Op, Val]) ->
    {N, _R, W} = ?NRW(System),
    SD = #state{req_id=ReqID,
                from=From,
                entity=Entity,
                op=Op,
                vnode=VNode,
                start=now(),
                w = W,
                n = N,
                system=System,
                cordinator=Node,
                val=Val},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{
                        entity = Entity,
                        n = N,
                        system = System
                       }) ->
    Bucket = list_to_binary(atom_to_list(System)),
    DocIdx = riak_core_util:chash_key({Bucket, term_to_binary(Entity)}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, System),
    SD = SD0#state{preflist=Preflist},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{req_id=ReqID,
                            entity=Entity,
                            op=Op,
                            val=Val,
                            vnode=VNode,
                            cordinator=Cordinator,
                            preflist=Preflist}) ->
    case Val of
        undefined ->
            VNode:Op(Preflist, {ReqID, Cordinator}, Entity);
        _ ->
            VNode:Op(Preflist, {ReqID, Cordinator}, Entity, Val)
    end,
    {next_state, waiting, SD0}.

%% @doc Wait for W write reqs to respond.
waiting({ok, ReqID}, SD0=#state{from=From, num_w=NumW0, req_id=ReqID, w = W}) ->
    NumW = NumW0 + 1,
    SD = SD0#state{num_w=NumW},
    lager:info("Write(~p) ok", [NumW]),
    if
        NumW =:= W ->
            case From of
                undefined ->
                    ok;
                _ ->
                    From ! {ReqID, ok}
            end,
            {stop, normal, SD};
        true -> {next_state, waiting, SD}
    end;

waiting({ok, ReqID, Reply},
        SD0=#state{from=From, num_w=NumW0, req_id=ReqID, w = W}) ->
    NumW = NumW0 + 1,
    SD = SD0#state{num_w=NumW},
    lager:info("Write(~p) reply: ~p", [NumW, Reply]),
    if
        NumW =:= W ->
            if
                is_pid(From) ->
                    From ! {ReqID, ok, Reply};
                true -> ok
            end,
            {stop, normal, SD};

        true -> {next_state, waiting, SD}
    end.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%%stat_name(snarl_user_vnode) ->
%%    "user";
%%stat_name(snarl_role_vnode) ->
%%    "role";
%%stat_name(snarl_org_vnode) ->
%%    "org";
%%stat_name(snarl_token_vnode) ->
%%    "token".
