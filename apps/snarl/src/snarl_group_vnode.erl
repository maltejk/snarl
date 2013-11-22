-module(snarl_group_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
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
         handle_exit/3,
         handle_info/2]).

-export([
         master/0,
         aae_repair/2,
         hashtree_pid/1,
         rehash/3,
         hash_object/2,
         request_hashtree_pid/1
        ]).

%% Reads
-export([get/3]).

%% Writes
-export([add/4,
         gc/4,
         set/4,
         import/4,
         delete/3,
         grant/4,
         repair/4,
         revoke/4,
         revoke_prefix/4]).

-ignore_xref([
              start_vnode/1,
              gc/4,
              get/3,
              add/4,
              delete/3,
              grant/4,
              set/4,
              import/4,
              repair/4,
              revoke/4,
              revoke_prefix/4,
              handle_info/2
             ]).


-record(state, {db, partition, node, hashtrees}).

-define(SERVICE, sniffle_group).

-define(MASTER, snarl_group_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(BKey, RObj) ->
    lager:debug("Hashing Key: ~p", [BKey]),
    list_to_binary(integer_to_list(erlang:phash2({BKey, RObj}))).

aae_repair(_, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    snarl_group:get_(Key).

hashtree_pid(Partition) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {hashtree_pid, node()},
                                        ?MASTER,
                                        infinity).

%% Asynchronous version of {@link hashtree_pid/1} that sends a message back to
%% the calling process. Used by the {@link riak_core_entropy_manager}.
request_hashtree_pid(Partition) ->
    ReqId = {hashtree_pid, Partition},
    riak_core_vnode_master:command({Partition, node()},
                                   {hashtree_pid, node()},
                                   {raw, ReqId, self()},
                                   ?MASTER).

%% Used by {@link riak_core_exchange_fsm} to force a vnode to update the hashtree
%% for repaired keys. Typically, repairing keys will trigger read repair that
%% will update the AAE hash in the write path. However, if the AAE tree is
%% divergent from the KV data, it is possible that AAE will try to repair keys
%% that do not have divergent KV replicas. In that case, read repair is never
%% triggered. Always rehashing keys after any attempt at repair ensures that
%% AAE does not try to repair the same non-divergent keys over and over.
rehash(Preflist, _, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {rehash, Key},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


repair(IdxNode, Group, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Group, VClock, Obj},
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

%%%===================================================================
%%% API - writes
%%%===================================================================

set(Preflist, ReqID, UUID, Attributes) ->
    riak_core_vnode_master:command(Preflist,
                                   {set, ReqID, UUID, Attributes},
                                   {fsm, undefined, self()},
                                   ?MASTER).

import(Preflist, ReqID, UUID, Import) ->
    riak_core_vnode_master:command(Preflist,
                                   {import, ReqID, UUID, Import},
                                   {fsm, undefined, self()},
                                   ?MASTER).

gc(Preflist, ReqID, UUID, GCable) ->
    riak_core_vnode_master:command(Preflist,
                                   {gc, ReqID, UUID, GCable},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add(Preflist, ReqID, UUID, Group) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, UUID, Group},
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

revoke_prefix(Preflist, ReqID, Group, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {revoke_prefix, ReqID, Group, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).
%%%===================================================================
%%% VNode
%%%===================================================================
init([Partition]) ->
    DB = list_to_atom(integer_to_list(Partition)),
    fifo_db:start(DB),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE,
                                                    Partition,
                                                    undefined),
    {ok, #state{db = DB, hashtrees = HT, partition = Partition, node = node()}}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Group, _VClock, #snarl_obj{} = Obj},
               _Sender, State) ->
    case fifo_db:get(State#state.db, <<"group">>, Group) of
        {ok, _O} when _O#snarl_obj.vclock =:= _VClock ->
            do_put(Group, Obj, State);
        not_found ->
            do_put(Group, Obj, State);
        _ ->
            lager:warning("[GRP:~s] Could not read repair, group changed.", [Group])
    end,
    {noreply, State};

%%%===================================================================
%%% AAE
%%%===================================================================

handle_command({hashtree_pid, Node}, _, State=#state{hashtrees=HT}) ->
    %% Handle riak_core request forwarding during ownership handoff.
    %% Following is necessary in cases where anti-entropy was enabled
    %% after the vnode was already running
    case {node(), HT} of
        {Node, undefined} ->
            HT1 =  riak_core_aae_vnode:maybe_create_hashtrees(
                     ?SERVICE,
                     State#state.partition,
                     HT),
            {reply, {ok, HT1}, State#state{hashtrees = HT1}};
        {Node, _} ->
            {reply, {ok, HT}, State};
        _ ->
            {reply, {error, wrong_node}, State}
    end;

handle_command({rehash, Key}, _, State=#state{db=DB}) ->
    case fifo_db:get(DB, <<"group">>, Key) of
        {ok, Term} ->
            riak_core_aae_vnode:update_hashtree(<<"group">>, Key,
                                                term_to_binary(Term),
                                                State#state.hashtrees);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete({<<"group">>, Key},
                                            State#state.hashtrees)
    end,
    {noreply, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    lager:debug("Fold on ~p", [State#state.partition]),
    Acc = fifo_db:fold(State#state.db, <<"group">>,
                       fun(K, V, O) ->
                               Fun({<<"group">>, K}, V, O)
                       end, Acc0),
    {reply, Acc, State};

%%%===================================================================
%%% General
%%%===================================================================

handle_command({get, ReqID, Group}, _Sender, State) ->
    Res = case fifo_db:get(State#state.db, <<"group">>, Group) of
              {ok, #snarl_obj{val = V0} = R} ->
                  R#snarl_obj{val = snarl_group_state:load(V0)};
              not_found ->
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({add, {ReqID, Coordinator} = ID, UUID, Group}, _Sender, State) ->
    Group0 = snarl_group_state:new(),
    Group1 = snarl_group_state:name(ID, Group, Group0),
    Group2 = snarl_group_state:uuid(ID, UUID, Group1),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    GroupObj = #snarl_obj{val=Group2, vclock=VC},
    do_put(UUID, GroupObj, State),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, Group}, _Sender, State) ->
    fifo_db:delete(State#state.db, <<"group">>, Group),
    {reply, {ok, ReqID}, State};

handle_command({set, {ReqID, Coordinator}, Group, Attributes}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"group">>, Group) of
        {ok, #snarl_obj{val=H0} = O} ->
            H1 = snarl_group_state:load(H0),
            H2 = lists:foldr(
                   fun ({Attribute, Value}, H) ->
                           snarl_group_state:set_metadata(Coordinator,
                                                          Attribute, Value, H)
                   end, H1, Attributes),
            GroupObj = snarl_obj:update(H2, Coordinator, O),
            do_put(Group, GroupObj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[groups] tried to write to a non existing group: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({import, {ReqID, Coordinator} = ID, UUID, Data}, _Sender, State) ->
    H1 = snarl_group_state:load(Data),
    H2 = snarl_group_state:uuid(ID, UUID, H1),
    case fifo_db:get(State#state.db, <<"group">>, UUID) of
        {ok, O} ->
            GroupObj = snarl_obj:update(H2, Coordinator, O),
            do_put(UUID, GroupObj, State),
            {reply, {ok, ReqID}, State};
        _R ->
            VC0 = vclock:fresh(),
            VC = vclock:increment(Coordinator, VC0),
            GroupObj = #snarl_obj{val=H2, vclock=VC},
            do_put(UUID, GroupObj, State),
            {reply, {ok, ReqID}, State}
    end;

handle_command({Action, {ReqID, Coordinator}, Group, Param1, Param2}, _Sender, State) ->
    change_group(Group, Action, [Param1, Param2], Coordinator, State, ReqID);

handle_command({Action, {ReqID, Coordinator}, Group, Param1}, _Sender, State) ->
    change_group(Group, Action, [Param1], Coordinator, State, ReqID);

handle_command(Message, _Sender, State) ->
    lager:error("[group] Unknown message: ~p", [Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#state.db, <<"group">>, Fun, Acc0),
    {reply, Acc, State};

handle_handoff_command({get, _ReqID, _Vm} = Req, Sender, State) ->
    handle_command(Req, Sender, State);

handle_handoff_command(Req, Sender, State) ->
    S1 = case handle_command(Req, Sender, State) of
             {noreply, NewState} ->
                 NewState;
             {reply, _, NewState} ->
                 NewState
         end,
    {forward, S1}.

handoff_starting(TargetNode, State) ->
    lager:warning("Starting handof to: ~p", [TargetNode]),
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Group, #snarl_obj{val = Vin} = Obj} = binary_to_term(Data),
    V = snarl_group_state:load(Vin),
    case fifo_db:get(State#state.db, <<"group">>, Group) of
        {ok, #snarl_obj{val = V0}} ->
            V1 = snarl_group_state:load(V0),
            GroupObj = Obj#snarl_obj{val = snarl_group_state:merge(V, V1)},
            do_put(Group, GroupObj, State);
        not_found ->
            VC0 = vclock:fresh(),
            VC = vclock:increment(node(), VC0),
            GroupObj = #snarl_obj{val=V, vclock=VC},
            do_put(Group, GroupObj, State)
    end,
    {reply, ok, State}.

encode_handoff_item(Group, Data) ->
    term_to_binary({Group, Data}).

is_empty(State) ->
    fifo_db:fold(State#state.db,
                 <<"group">>,
                 fun (_,_, _) ->
                         {false, State}
                 end, {true, State}).

delete(State) ->
    Trans = fifo_db:fold(State#state.db,
                         <<"group">>,
                         fun (K,_, A) ->
                                 [{delete, <<"group", K/binary>>} | A]
                         end, []),
    fifo_db:transact(State#state.db, Trans),
    {ok, State}.

handle_coverage({lookup, Name}, _KeySpaces, {_, ReqID, _}, State) ->
    Res = fifo_db:fold(State#state.db,
                       <<"group">>,
                       fun (UUID, #snarl_obj{val=G0}, not_found) ->
                               G1 = snarl_group_state:load(G0),
                               case snarl_group_state:name(G1) of
                                   Name ->
                                       UUID;
                                   _ ->
                                       not_found
                               end;
                           (_U, _, Res) ->
                               Res
                       end, not_found),
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, [Res]},
     State};

handle_coverage({list, Requirements}, _KeySpaces, {_, ReqID, _}, State) ->
    Getter = fun(#snarl_obj{val=S0}, <<"uuid">>) ->
                     snarl_group_state:uuid(snarl_group_state:load(S0))
             end,
    List = fifo_db:fold(State#state.db,
                        <<"group">>,
                        fun (Key, E, C) ->
                                case rankmatcher:match(E, Getter, Requirements) of
                                    false ->
                                        C;
                                    Pts ->
                                        [{Pts, Key} | C]
                                end
                        end, []),
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, List},
     State};

handle_coverage(list, _KeySpaces, {_, ReqID, _}, State) ->
    List = fifo_db:fold(State#state.db,
                        <<"group">>,
                        fun (K, _, L) ->
                                [K|L]
                        end, []),
    {reply,
     {ok, ReqID, {State#state.partition,State#state.node}, List},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

change_group(Group, Action, Vals, Coordinator, State, ReqID) ->
    case fifo_db:get(State#state.db, <<"group">>, Group) of
        {ok, #snarl_obj{val=H0} = O} ->
            H1 = snarl_group_state:load(H0),
            ID = {ReqID, Coordinator},
            H2 = case Vals of
                     [Val] ->
                         snarl_group_state:Action(ID, Val, H1);
                     [Val1, Val2] ->
                         snarl_group_state:Action(ID, Val1, Val2, H1)
                 end,

            GroupObj = snarl_obj:update(H2, Coordinator, O),
            do_put(Group, GroupObj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[groups] tried to write to a non existing group: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end.

%%%===================================================================
%%% AAE
%%%===================================================================

handle_info(retry_create_hashtree, State=#state{hashtrees=undefined}) ->
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE, State#state.partition,
                                                    undefined),
    {ok, State#state{hashtrees = HT}};
handle_info(retry_create_hashtree, State) ->
    {ok, State};
handle_info({'DOWN', _, _, Pid, _}, State=#state{hashtrees=Pid}) ->
    HT = riak_core_aae_vnode:maybe_create_hashtrees(?SERVICE, State#state.partition,
                                                    Pid),
    {ok, State#state{hashtrees = HT}};
handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};
handle_info(_, State) ->
    {ok, State}.

do_put(Key, Obj, State) ->
    fifo_db:put(State#state.db, <<"group">>, Key, Obj),
    riak_core_aae_vnode:update_hashtree(<<"group">>, Key, term_to_binary(Obj),
                                        State#state.hashtrees).
