-module(snarl_group_vnode).
-behaviour(riak_core_vnode).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").


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

% Reads
-export([list/2,
	 get/3]).

% Writes
-export([add/3,
	 delete/3,
	 grant/4,
	 repair/3,
	 revoke/4]).

-record(state, {partition, groups=[], index=[], dbref, node}).

-define(MASTER, snarl_group_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


repair(IdxNode, Group, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, undefined, Group, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Group) ->
    riak_core_vnode_master:command(Preflist,
				   {get, ReqID, Group},
				   {fsm, undefined, self()},
				   ?MASTER).

list(Preflist, ReqID) ->
    riak_core_vnode_master:coverage(
      {list, ReqID},
      Preflist,
      all,
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
    {ok, DBLoc} = application:get_env(snarl, db_path),
    {ok, DBRef} = eleveldb:open(DBLoc ++ "/groups."++integer_to_list(Partition)++".ldb", [{create_if_missing, true}]),

    {ok, DBRef} = eleveldb:open("groups."++integer_to_list(Partition)++".ldb", [{create_if_missing, true}]),
    {Index, Groups} = read_groups(DBRef),
    {ok, #state {
       index = Index,
       node = node(),
       groups = Groups,
       partition = Partition,
       dbref = DBRef}}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, undefined, Group, Obj}, _Sender, #state{groups=Groups0}=State) ->
    lager:warning("repair performed ~p~n", [Obj]),
    Groups1 = dict:store(Group, Obj, Groups0),
    {noreply, State#state{groups=Groups1}};


handle_command({add, {ReqID, Coordinator}, Group}, _Sender,
	       #state{groups=Groups,  dbref=DBRef, index=Index0} = State) ->
    Group0 = statebox:new(fun snarl_group_state:new/0),
    Group1 = statebox:modify({snarl_group_state, name, [Group]}, Group0),
    Index1 = snarl_group_state:add(Group, Index0),
    eleveldb:put(DBRef, <<"#groups">>, term_to_binary(Index1), []),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    GroupObj = #snarl_obj{val=Group1, vclock=VC},
    {ok, Groups1} = add_group(Group, GroupObj, Groups, DBRef),
    {reply, {ok, ReqID}, State#state{groups=Groups1, index = Index1}};

handle_command({delete, {ReqID, _Coordinator}, Group}, _Sender,
	       #state{groups=Groups, dbref=DBRef, index=Index0} = State) ->
    Groups1 = dict:erase(Group, Groups),
    Index1 = snarl_group_state:delete(Group, Index0),
    eleveldb:put(DBRef, <<"#groups">>, term_to_binary(Index1), []),
    eleveldb:delete(DBRef, Group, []),
    {reply, {ok, ReqID}, State#state{groups=Groups1, index=Index1}};

handle_command({get, ReqID, Group}, _Sender, #state{groups=Groups, partition=Partition, node=Node} = State) ->
    Res = case dict:find(Group, Groups) of
	      error ->
		  {ok, ReqID, {Partition,Node}, not_found};
	      {ok, V} ->
		  {ok, ReqID, {Partition,Node}, V}
	  end,
    {reply, Res, State};

handle_command({Action, {ReqID, Coordinator}, Group, Passwd}, _Sender,
	       #state{groups=Groups, dbref=DBRef} = State) ->
    Groups1 = change_group_callback(Group, Action, Passwd, Coordinator, Groups, DBRef),
    {reply, {ok, ReqID}, State#state{groups=Groups1}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.


handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = dict:fold(Fun, Acc0, State#state.groups),
    {reply, Acc, State};

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(TargetNode, State) ->
    lager:warning("Starting handof to: ~p", [TargetNode]),
    {true, State}.

handoff_cancelled(State) ->
    {Index, Groups} = read_groups(State#state.dbref),
    {ok, State#state{groups=Groups, index=Index}}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, #state{dbref=DBRef} = State) ->
    {Group, Data} = binary_to_term(Data),
    Index1 = snarl_group_state:add(Group, State#state.index),
    eleveldb:put(DBRef, <<"#groups">>, term_to_binary(Index1), []),
    {ok, Groups1} = add_group(Group, Data, State#state.groups, DBRef),
    {reply, ok, State#state{groups = Groups1, index=Index1}}.

encode_handoff_item(Group, Data) ->
    term_to_binary({Group, Data}).

is_empty(State) ->
    case dict:size(State#state.groups) of
	0 ->
	    {true, State};
	_ ->
	    {true, State}
    end.

delete(#state{dbref=DBRef} = State) ->
    eleveldb:close(DBRef),
    eleveldb:destroy("groups."++integer_to_list(State#state.partition)++".ldb",[]),
    {ok, DBRef1} = eleveldb:open("groups."++integer_to_list(State#state.partition)++".ldb", [{create_if_missing, true}]),
    {ok, State#state{dbref=DBRef1, groups=dict:new(), index=[]}}.

handle_coverage({list, ReqID}, _KeySpaces, _Sender,
		#state{index=Index, partition=Partition, node=Node} = State) ->
    ?PRINT({handle_coverage, {list, ReqID}}),
    Res = {ok, ReqID, {Partition,Node}, Index},
    {reply, Res, State};


handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.


terminate(_Reason, #state{dbref=undefined} = _State) ->
    ok;

terminate(_Reason, #state{dbref=DBRef} = _State) ->
    eleveldb:close(DBRef),
    ok.

read_groups(DBRef) ->
    case eleveldb:get(DBRef, <<"#groups">>, []) of
	not_found ->
	    {[], dict:new()};
	{ok, Bin} ->
	    Index = binary_to_term(Bin),
	    {Index,
	     lists:foldl(fun (Group, Groups0) ->
				 {ok, GrBin} = eleveldb:get(DBRef, Group, []),
				 dict:store(Group, binary_to_term(GrBin), Groups0)
			 end, dict:new(), Index)}
    end.

add_group(Group, GroupData, Groups, DBRef) ->
    Groups1 = dict:store(Group, GroupData, Groups),
    eleveldb:put(DBRef, Group, term_to_binary(GroupData), []),
    {ok, Groups1}.

change_group_callback(Group, Action, Val, Coordinator, Groups, DBRef) ->
    Groups1 = dict:update(Group, update_group(Coordinator, Val, Action), Groups),
    {ok, GroupData} = dict:find(Group, Groups1),
    eleveldb:put(DBRef, Group, term_to_binary(GroupData), []),
    Groups1.

update_group(Coordinator, Val, Action) ->
    fun (#snarl_obj{val=Group0}=O) ->
	    Group1 = statebox:modify({snarl_group_state, Action, [Val]}, Group0),
	    Group2 = statebox:expire(?STATEBOX_EXPIRE, Group1),
	    snarl_obj:update(Group2, Coordinator, O)
    end.
