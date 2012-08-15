-module(snarl_user_vnode).
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

-record(state, {partition, users=[]}).

-define(MASTER, snarl_user_vnode_master).

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

read(Preflist, ReqID, User) ->
    ?PRINT({get, Preflist, ReqID, User}),

    riak_core_vnode_master:command(Preflist,
				   {get, ReqID, User},
				   {fsm, undefined, self()},
				   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

add(Preflist, ReqID, User) ->
    riak_core_vnode_master:command(Preflist,
				   {add, ReqID, User},
				   {fsm, undefined, self()},
				   ?MASTER).

delete(Preflist, ReqID, User) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, User},
				   {fsm, undefined, self()},
                                   ?MASTER).

grant(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, User, Val},
				   {fsm, undefined, self()},

                                   ?MASTER).

revoke(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, User, Val},
				   {fsm, undefined, self()},
                                   ?MASTER).

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};


handle_command({add, ReqID, User}, _Sender, #state{users=Users} = State) ->
    ?PRINT({add, ReqID, User}),
    {reply, {ok, ReqID}, State#state{users=[User|Users]}};

handle_command({delete, ReqID, User}, _Sender, #state{users=Users} = State) ->
    ?PRINT({delete, ReqID, User}),
    {reply, {ok, ReqID}, State#state{users=lists:delete(User, Users)}};

handle_command({grant, ReqID, User, Permission}, _Sender, State) ->
    ?PRINT({grant, ReqID, User, Permission}),
    {reply, {ok, ReqID}, State};

handle_command({revoke, ReqID, User, Permission}, _Sender, State) ->
    ?PRINT({delete, ReqID, User, Permission}),
    {reply, {ok, ReqID}, State};

handle_command({get, ReqID, User}, _Sender, #state{users=Users} = State) ->
    ?PRINT({get, ReqID, User}),
    {reply, {ok, ReqID, Users}, State};

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
