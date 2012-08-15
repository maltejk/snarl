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

-export([list/2,
	 get/3,
	 add/3,
	 delete/3,
	 grant/4,
	 revoke/4]).

-record(state, {partition, groups=[], dbref}).

-define(MASTER, snarl_group_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Group) ->
    riak_core_vnode_master:command(Preflist,
				   {get, ReqID, Group},
				   {fsm, undefined, self()},
				   ?MASTER).

list(Preflist, ReqID) ->
    riak_core_vnode_master:command(Preflist,
				   {list, ReqID},
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
                                   {delete, ReqID, Group},
				   {fsm, undefined, self()},
                                   ?MASTER).

grant(Preflist, ReqID, Group, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {grant, ReqID, Group, Val},
				   {fsm, undefined, self()},

                                   ?MASTER).

revoke(Preflist, ReqID, Group, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {revoke, ReqID, Group, Val},
				   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================


init([Partition]) ->
    {ok, DBRef} = eleveldb:open("groups."++integer_to_list(Partition)++".ldb", [{create_if_missing, true}]),
    Groups = case eleveldb:get(DBRef, <<"#groups">>, []) of
		 not_found -> 
		     dict:new();
		 {ok, Bin} ->
		     lists:foldl(fun (Group, Groups0) ->
					{ok, GrBin} = eleveldb:get(DBRef, list_to_binary(Group), []),
					dict:store(Group, binary_to_term(GrBin), Groups0)
				end, dict:new(), binary_to_term(Bin))
	     end,
    {ok, #state { 
       groups=Groups,
       partition=Partition,
       dbref=DBRef}}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};


handle_command({add, ReqID, Group}, _Sender, #state{groups=Groups, dbref=DBRef} = State) ->
    ?PRINT({add, ReqID, Group, Groups}),
    Groups1 = dict:store(Group,[],Groups),
    eleveldb:put(DBRef, <<"#groups">>, term_to_binary(dict:fetch_keys(Groups1)), []),
    eleveldb:put(DBRef, list_to_binary(Group), term_to_binary([]), []),
    {reply, {ok, ReqID}, State#state{groups=Groups1}};

handle_command({delete, ReqID, Group}, _Sender, #state{groups=Groups, dbref=DBRef} = State) ->
    ?PRINT({delete, ReqID, Group}),
    Groups1 = dict:erase(Group, Groups),
    eleveldb:put(DBRef, <<"#groups">>, term_to_binary(dict:fetch_keys(Groups1)), []),
    eleveldb:delete(DBRef, list_to_binary(Group), []),
    {reply, {ok, ReqID}, State#state{groups=Groups1}};

handle_command({grant, ReqID, Group, Permission}, _Sender, #state{groups=Groups, dbref=DBRef} = State) ->
    ?PRINT({grant, ReqID, Group, Permission}),
    Groups1 = dict:update(Group,
			  fun (L) ->
				  [Permission|L]
			  end, Groups),
    {ok, Perms} = dict:find(Group, Groups1),
    eleveldb:put(DBRef, list_to_binary(Group), term_to_binary(Perms), []),
    {reply, {ok, ReqID}, State#state{groups=Groups1}};

handle_command({revoke, ReqID, Group, Permission}, _Sender, #state{groups=Groups, dbref=DBRef} = State) ->
    ?PRINT({delete, ReqID, Group, Permission}),
    Groups1 = dict:update(Group,
			  fun (L) ->
				  lists:delete(Permission, L)
			  end, Groups),
    eleveldb:put(DBRef, list_to_binary(Group), term_to_binary(Groups1), []),
    {reply, {ok, ReqID}, State};

handle_command({list, ReqID}, _Sender, #state{groups=Groups} = State) ->
    ?PRINT({list, ReqID}),
    {reply, {ok, ReqID, dict:fetch_keys(Groups)}, State};

handle_command({get, ReqID, Group}, _Sender, #state{groups=Groups} = State) ->
    ?PRINT({get, ReqID, Group}),
    Res = case dict:find(Group, Groups) of
	      error ->
		  {error, ReqID, not_found};
	      {ok, V} ->
		  {ok, ReqID, V}
	  end,
    {reply, Res, State};

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

terminate(_Reason, #state{dbref=DBRef} = _State) ->
    eleveldb:close(DBRef),
    ok.
