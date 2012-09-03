-module(snarl_user_vnode).
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
	 passwd/4,
	 join/4,
	 leave/4,
	 grant/4,
	 repair/3,
	 revoke/4]).

-record(state, {partition, users=[], index=[], dbref, node}).

-define(MASTER, snarl_user_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


repair(IdxNode, User, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, undefined, User, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, User) ->
    riak_core_vnode_master:command(Preflist,
				   {get, ReqID, User},
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

add(Preflist, ReqID, User) ->
    riak_core_vnode_master:command(Preflist,
				   {add, ReqID, User},
				   {fsm, undefined, self()},
				   ?MASTER).

delete(Preflist, ReqID, User) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, User},
				   {fsm, undefined, self()},
                                   ?MASTER).

passwd(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {passwd, ReqID, User, Val},
				   {fsm, undefined, self()},
                                   ?MASTER).


join(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {join, ReqID, User, Val},
				   {fsm, undefined, self()},
                                   ?MASTER).

leave(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {leave, ReqID, User, Val},
				   {fsm, undefined, self()},
                                   ?MASTER).

grant(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {grant, ReqID, User, Val},
				   {fsm, undefined, self()},
                                   ?MASTER).

revoke(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {revoke, ReqID, User, Val},
				   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================
    
init([Partition]) ->
    {ok, DBRef} = eleveldb:open("users."++integer_to_list(Partition)++".ldb", [{create_if_missing, true}]),
    {Index, Users} = read_users(DBRef),
    {ok, #state { 
       index = Index,
       node = node(),
       users = Users,
       partition = Partition,
       dbref = DBRef}}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, undefined, User, Obj}, _Sender, #state{users=Users0}=State) ->
    lager:warning("repair performed ~p~n", [Obj]),
    Users1 = dict:store(User, Obj, Users0),
    {noreply, State#state{users=Users1}};


handle_command({add, {ReqID, Coordinator}, User}, _Sender,
	       #state{users=Users,  dbref=DBRef, index=Index0} = State) ->
    Index1 = snarl_user_state:add(User, Index0),
    eleveldb:put(DBRef, <<"#users">>, term_to_binary(Index1), []),
    User0 = statebox:new(fun snarl_user_state:new/0),
    User1 = statebox:modify({fun snarl_user_state:name/2, [User]}, User0),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    UserObj = #snarl_obj{val=User1, vclock=VC},
    {ok, Users1} = add_user(User, UserObj, Users, DBRef),
    {reply, {ok, ReqID}, State#state{users=Users1, index = Index1}};

handle_command({delete, {ReqID, _Coordinator}, User}, _Sender, 
	       #state{users=Users, dbref=DBRef, index=Index0} = State) ->
    Users1 = dict:erase(User, Users),
    Index1 = snarl_user_state:delete(User, Index0),
    eleveldb:put(DBRef, <<"#users">>, term_to_binary(Index1), []),
    eleveldb:delete(DBRef, User, []),
    {reply, {ok, ReqID}, State#state{users=Users1, index=Index1}};

handle_command({join = Action, {ReqID, Coordinator}, User, Group}, _Sender, 
	       #state{users=Users, dbref=DBRef} = State) ->
    case snarl_group:get(Group) of
	{ok, not_found} ->
	    {reply, {ok, ReqID, not_found}, State};
	{ok, _} ->
	    Users1 = change_user_callback(User, Action, Group, Coordinator, Users, DBRef),
	    {reply, {ok, ReqID, joined}, State#state{users=Users1}}
    end;

handle_command({get, ReqID, User}, _Sender, #state{users=Users, partition=Partition, node=Node} = State) ->
    Res = case dict:find(User, Users) of
	      error ->
		  {ok, ReqID, {Partition,Node}, not_found};
	      {ok, V} ->
		  {ok, ReqID, {Partition,Node}, V}
	  end,
    {reply, Res, State};

handle_command({Action, {ReqID, Coordinator}, User, Passwd}, _Sender, 
	       #state{users=Users, dbref=DBRef} = State) ->
    Users1 = change_user_callback(User, Action, Passwd, Coordinator, Users, DBRef),
    {reply, {ok, ReqID}, State#state{users=Users1}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.


handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = dict:fold(Fun, Acc0, State#state.users),
    {reply, Acc, State};

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(TargetNode, State) ->
    lager:warning("Starting handof to: ~p", [TargetNode]),
    {true, State}.

handoff_cancelled(State) ->
    {Index, Users} = read_users(State#state.dbref),
    {ok, State#state{users=Users, index=Index}}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, #state{dbref=DBRef} = State) ->
    {User, UserData} = binary_to_term(Data),
    Index1 = snarl_user_state:add(User, State#state.index),
    eleveldb:put(DBRef, <<"#users">>, term_to_binary(Index1), []),
    {ok, Users1} = add_user(User, UserData, State#state.users, DBRef),
    {reply, ok, State#state{users = Users1}}.

encode_handoff_item(User, Data) ->
    term_to_binary({User, Data}).

is_empty(State) ->
    case dict:size(State#state.users) of
	0 ->
	    {true, State};
	_ ->
	    {true, State}
    end.

delete(#state{dbref=DBRef} = State) ->
    eleveldb:close(DBRef),
    eleveldb:destroy("users."++integer_to_list(State#state.partition)++".ldb",[]),
    {ok, DBRef1} = eleveldb:open("users."++integer_to_list(State#state.partition)++".ldb", [{create_if_missing, true}]),
    {ok, State#state{dbref=DBRef1, index=[], users=dict:new()}}.

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

read_users(DBRef) ->
    case eleveldb:get(DBRef, <<"#users">>, []) of
	not_found -> 
	    {[], dict:new()};
	{ok, Bin} ->
	    Index = binary_to_term(Bin),
	    {Index,
	     lists:foldl(fun (User, Users0) ->
				 {ok, GrBin} = eleveldb:get(DBRef, User, []),
				 dict:store(User, binary_to_term(GrBin), Users0)
			 end, dict:new(), Index)}
    end.

add_user(User, UserData, Users, DBRef) ->
    Users1 = dict:store(User, UserData, Users),
    eleveldb:put(DBRef, User, term_to_binary(UserData), []),
    {ok, Users1}.

change_user_callback(User, Action, Val, Coordinator, Users, DBRef) ->    
    Users1 = dict:update(User, update_user(Coordinator, Val, Action), Users),
    {ok, UserData} = dict:find(User, Users1),
    eleveldb:put(DBRef, User, term_to_binary(UserData), []),
    Users1.

update_user(Coordinator, Val, Action) ->
    fun (#snarl_obj{val=User0}=O) ->
	    User1 = statebox:modify({fun snarl_user_state:Action/2, [Val]}, User0),
	    User2 = statebox:expire(?STATEBOX_EXPIRE, User1),
	    snarl_obj:update(User2, Coordinator, O)
    end.
