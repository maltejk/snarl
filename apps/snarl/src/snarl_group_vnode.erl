-module(snarl_group_vnode).
-behaviour(riak_core_vnode).
-include("snarl.hrl").

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

-export([
	 read/3,
	 add/3,
	 delete/3,
	 grant/4,
	 revoke/4]).

-record(state, {partition, groups=[]}).

-define(MASTER, snarl_group_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state { partition=Partition }}.

%%%===================================================================
%%% API - reads
%%%===================================================================

read(Preflist, ReqID, Group) ->
    ?PRINT({get, Preflist, ReqID, Group}),

    riak_core_vnode_master:command(Preflist,
				   {get, ReqID, Group},
				   {fsm, undefined, self()},
				   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

add(Preflist, ReqID, Group) ->
    riak_core_vnode_master:command(Preflist,
				   {add, ReqID, Group},
				   {fsm, undefined, self()},
				   ?MASTER).

delete(Preflist, ReqID, Group) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Group},
				   {fsm, undefined, self()},
                                   ?MASTER).

grant(Preflist, ReqID, Group, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Group, Val},
				   {fsm, undefined, self()},

                                   ?MASTER).

revoke(Preflist, ReqID, Group, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, Group, Val},
				   {fsm, undefined, self()},
                                   ?MASTER).

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};


handle_command({add, ReqID, Group}, _Sender, #state{groups=Groups} = State) ->
    ?PRINT({add, ReqID, Group}),
    {reply, {ok, ReqID}, State#state{groups=[Group|Groups]}};

handle_command({delete, ReqID, Group}, _Sender, #state{groups=Groups} = State) ->
    ?PRINT({delete, ReqID, Group}),
    {reply, {ok, ReqID}, State#state{groups=lists:delete(Group, Groups)}};

handle_command({grant, ReqID, Group, Permission}, _Sender, State) ->
    ?PRINT({grant, ReqID, Group, Permission}),
    {reply, {ok, ReqID}, State};

handle_command({revoke, ReqID, Group, Permission}, _Sender, State) ->
    ?PRINT({delete, ReqID, Group, Permission}),
    {reply, {ok, ReqID}, State};

handle_command({get, ReqID, Group}, _Sender, #state{groups=Groups} = State) ->
    ?PRINT({get, ReqID, Group}),
    {reply, {ok, ReqID, Groups}, State};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
