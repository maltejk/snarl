%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(snarl_group_write_fsm).
-behavior(gen_fsm).
-include("snarl.hrl").

%% API
-export([start_link/4, start_link/5, write/2, write/3]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2]).

%% req_id: The request id so the caller can verify the response.
%%
%% from: The pid of the sender so a reply can be made.
%%
%% group: The group.
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
		group :: string(),
                op :: atom(),
                val = undefined :: term() | undefined,
                preflist :: riak_core_apl:preflist2(),
                num_w = 0 :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, From, Group, Op) ->
    start_link(ReqID, From, Group, Op, undefined).

start_link(ReqID, From, Group, Op, Val) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, Group, Op, Val], []).

write(Group, Op) ->
    write(Group, Op, undefined).

write(Group, Op, Val) ->
    ReqID = mk_reqid(),
    snarl_group_write_fsm_sup:start_write_fsm([ReqID, self(), Group, Op, Val]),
    {ok, ReqID}.

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Group, Op, Val]) ->
    SD = #state{req_id=ReqID,
                from=From,
                group=Group,
                op=Op,
                val=Val},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{group=Group}) ->
    DocIdx = riak_core_util:chash_key({<<"group">>, list_to_binary(Group)}),
    Preflist = riak_core_apl:get_apl(DocIdx, ?N, snarl_group),
    SD = SD0#state{preflist=Preflist},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{req_id=ReqID,
                            group=Group,
                            op=Op,
                            val=Val,
                            preflist=Preflist}) ->
    case Val of
        undefined ->
            snarl_group_vnode:Op(Preflist, ReqID, Group);
        _ ->
            snarl_group_vnode:Op(Preflist, ReqID, Group, Val)
    end,
    {next_state, waiting, SD0}.

%% @doc Wait for W write reqs to respond.
waiting({ok, ReqID}, SD0=#state{from=From, num_w=NumW0}) ->
    NumW = NumW0 + 1,
    SD = SD0#state{num_w=NumW},
    if
        NumW =:= ?W ->
            From ! {ReqID, ok},
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

mk_reqid() -> erlang:phash2(erlang:now()).
