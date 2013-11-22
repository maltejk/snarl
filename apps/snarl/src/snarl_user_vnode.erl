-module(snarl_user_vnode).
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
         handle_info/2,
         handle_exit/3]).

-export([
         master/0,
         aae_repair/2,
         hashtree_pid/1,
         rehash/3,
         hash_object/2,
         request_hashtree_pid/1
        ]).

%% Reads
-export([
         get/3
        ]).

%% Writes
-export([
         add/4,
         import/4,
         repair/4,
         add_key/4,
         delete/3,
         grant/4, revoke/4, revoke_prefix/4,
         join/4, leave/4,
         passwd/4,
         join_org/4, leave_org/4, select_org/4,
         revoke_key/4,
         set/4
        ]).

-ignore_xref([
              aae_get/2,
              hashtree_pid/1,
              rehash/2,
              request_hashtree_pid/1,
              add/4,
              add_key/4,
              find_key/3,
              delete/3,
              get/3,
              grant/4,
              import/4,
              join/4,
              leave/4,
              list/2,
              list/3,
              lookup/3,
              passwd/4,
              repair/4,
              revoke/4,
              revoke_key/4,
              revoke_prefix/4,
              set/4,
              handle_info/2,
              join_org/4, leave_org/4, select_org/4,
              start_vnode/1
             ]).


-record(state, {db, partition, node, hashtrees}).

-define(SERVICE, sniffle_user).

-define(MASTER, snarl_user_vnode_master).

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
    snarl_user:get_(Key).

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


repair(IdxNode, User, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, User, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================g

get(Preflist, ReqID, User) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, User},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

add(Preflist, ReqID, UUID, User) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, UUID, User},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add_key(Preflist, ReqID, UUID, {KeyId, Key}) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_key, ReqID, UUID, KeyId, Key},
                                   {fsm, undefined, self()},
                                   ?MASTER).

revoke_key(Preflist, ReqID, UUID, KeyId) ->
    riak_core_vnode_master:command(Preflist,
                                   {revoke_key, ReqID, UUID, KeyId},
                                   {fsm, undefined, self()},
                                   ?MASTER).

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

delete(Preflist, ReqID, User) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, User},
                                   {fsm, undefined, self()},
                                   ?MASTER).

passwd(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {password, ReqID, User, Val},
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

join_org(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {join_org, ReqID, User, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

leave_org(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {leave_org, ReqID, User, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

select_org(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {select_org, ReqID, User, Val},
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

revoke_prefix(Preflist, ReqID, User, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {revoke_prefix, ReqID, User, Val},
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

handle_command({repair, User, _VClock, #snarl_obj{} = Obj},
               _Sender, State) ->
    case fifo_db:get(State#state.db, <<"user">>, User) of
        {ok, _O} when _O#snarl_obj.vclock =:= _VClock ->
            do_put(User, Obj,  State);
        not_found ->
            do_put(User, Obj,  State);
        _ ->
            lager:warning("[USR:~s] Could not read repair, user changed.", [User])
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
    case fifo_db:get(DB, <<"user">>, Key) of
        {ok, Term} ->
            riak_core_aae_vnode:update_hashtree(<<"user">>, Key,
                                                term_to_binary(Term),
                                                State#state.hashtrees);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete({<<"user">>, Key},
                                            State#state.hashtrees)
    end,
    {noreply, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    lager:debug("Fold on ~p", [State#state.partition]),
    Acc = fifo_db:fold(State#state.db, <<"user">>,
                       fun(K, V, O) ->
                               Fun({<<"user">>, K}, V, O)
                       end, Acc0),
    {reply, Acc, State};

%%%===================================================================
%%% General
%%%===================================================================

handle_command({get, ReqID, User}, _Sender, State) ->
    Res = case fifo_db:get(State#state.db, <<"user">>, User) of
              {ok, #snarl_obj{val = V0} = R} ->
                  R#snarl_obj{val = snarl_user_state:load(V0)};
              not_found ->
                  not_found
          end,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({add, {ReqID, Coordinator} = ID, UUID, User}, _Sender, State) ->
    User0 = snarl_user_state:new(),
    User1 = snarl_user_state:name(ID, User, User0),
    User2 = snarl_user_state:uuid(ID, UUID, User1),
    User3 = snarl_user_state:grant(ID, [<<"users">>, UUID, <<"...">>], User2),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    UserObj = #snarl_obj{val=User3, vclock=VC},
    do_put(UUID, UserObj, State),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, User}, _Sender, State) ->
    fifo_db:delete(State#state.db, <<"user">>, User),
    riak_core_index_hashtree:delete({<<"user">>, User}, State#state.hashtrees),
    {reply, {ok, ReqID}, State};

handle_command({join = Action, {ReqID, Coordinator}, User, Group}, _Sender, State) ->
    case snarl_group:get(Group) of
        not_found ->
            {reply, {ok, ReqID, not_found}, State};
        {ok, _GroupObj} ->
            change_user(User, Action, [Group], Coordinator, State, ReqID)
    end;

handle_command({set, {ReqID, Coordinator}, User, Attributes}, _Sender, State) ->
    case fifo_db:get(State#state.db, <<"user">>, User) of
        {ok, #snarl_obj{val=H0} = O} ->
            H1 = snarl_user_state:load(H0),
            H2 = lists:foldr(
                   fun ({Attribute, Value}, H) ->
                           snarl_user_state:set_metadata(Coordinator,
                                                         Attribute, Value, H)
                   end, H1, Attributes),
            Obj = snarl_obj:update(H2, Coordinator, O),
            do_put(User, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[users] tried to write to a non existing user: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({import, {ReqID, Coordinator} = ID, UUID, Data}, _Sender, State) ->
    H1 = snarl_user_state:load(Data),
    H2 = snarl_user_state:uuid(ID, UUID, H1),
    case fifo_db:get(State#state.db, <<"user">>, UUID) of
        {ok, O} ->
            UserObj = snarl_obj:update(H2, Coordinator, O),
            do_put(UUID, UserObj, State),
            {reply, {ok, ReqID}, State};
        _R ->
            VC0 = vclock:fresh(),
            VC = vclock:increment(Coordinator, VC0),
            UserObj = #snarl_obj{val=H2, vclock=VC},
            do_put(UUID, UserObj, State),
            {reply, {ok, ReqID}, State}
    end;

handle_command({Action, {ReqID, Coordinator}, User, Param1, Param2}, _Sender, State) ->
    change_user(User, Action, [Param1, Param2], Coordinator, State, ReqID);

handle_command({Action, {ReqID, Coordinator}, User, Param}, _Sender, State) ->
    change_user(User, Action, [Param], Coordinator, State, ReqID);

handle_command(Message, _Sender, State) ->
    lager:error("[user] Unknown message: ~p", [Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#state.db, <<"user">>, Fun, Acc0),
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
    {User, #snarl_obj{val = Vin} = Obj} = binary_to_term(Data),
    V = snarl_user_state:load(Vin),
    case fifo_db:get(State#state.db, <<"user">>, User) of
        {ok, #snarl_obj{val = V0}} ->
            V1 = snarl_user_state:load(V0),
            UserObj = Obj#snarl_obj{val = snarl_user_state:merge(V, V1)},
            do_put(User, UserObj, State);
        not_found ->
            VC0 = vclock:fresh(),
            VC = vclock:increment(node(), VC0),
            UserObj = #snarl_obj{val=V, vclock=VC},
            do_put(User, UserObj, State)
    end,
    {reply, ok, State}.

encode_handoff_item(User, Data) ->
    term_to_binary({User, Data}).

is_empty(State) ->
    fifo_db:fold(State#state.db,
                 <<"user">>,
                 fun (_,_, _) ->
                         {false, State}
                 end, {true, State}).

delete(State) ->
    Trans = fifo_db:fold(State#state.db,
                         <<"user">>,
                         fun (K,_, A) ->
                                 [{delete, <<"user", K/binary>>} | A]
                         end, []),
    fifo_db:transact(State#state.db, Trans),
    {ok, State}.

handle_coverage({find_key, KeyID}, _KeySpaces, {_, ReqID, _}, State) ->
    Res = fifo_db:fold(State#state.db,
                       <<"user">>,
                       fun (UUID, #snarl_obj{val=U0}, not_found) ->
                               U1 = snarl_user_state:load(U0),
                               Ks = snarl_user_state:keys(U1),
                               Ks1 = [key_to_id(K) || {_, K} <- Ks],
                               case lists:member(KeyID, Ks1) of
                                   true ->
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

handle_coverage({lookup, Name}, _KeySpaces, {_, ReqID, _}, State) ->
    Res = fifo_db:fold(State#state.db,
                       <<"user">>,
                       fun (UUID, #snarl_obj{val = U0}, not_found) ->
                               U1 = snarl_user_state:load(U0),
                               case snarl_user_state:name(U1) of
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

handle_coverage(list, _KeySpaces, {_, ReqID, _}, State) ->
    List = fifo_db:fold(State#state.db,
                        <<"user">>,
                        fun (K, _, L) ->
                                [K|L]
                        end, []),
    {reply,
     {ok, ReqID, {State#state.partition,State#state.node}, List},
     State};

handle_coverage({list, Requirements}, _KeySpaces, {_, ReqID, _}, State) ->
    Getter = fun(#snarl_obj{val=S0}, <<"uuid">>) ->
                     snarl_user_state:uuid(snarl_user_state:load(S0))
             end,
    List = fifo_db:fold(State#state.db,
                        <<"user">>,
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

handle_coverage(Req, _KeySpaces, _Sender, State) ->
    lager:warning("Unknown coverage request: ~p", [Req]),
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

change_user(User, Action, Vals, Coordinator, State, ReqID) ->
    case fifo_db:get(State#state.db, <<"user">>, User) of
        {ok, #snarl_obj{val=H0} = O} ->
            H1 = snarl_user_state:load(H0),
            ID = {ReqID, Coordinator},
            H2 = case Vals of
                     [Val] ->
                         snarl_user_state:Action(ID, Val, H1);
                     [Val1, Val2] ->
                         snarl_user_state:Action(ID, Val1, Val2, H1)
                 end,
            UserObj = snarl_obj:update(H2, Coordinator, O),
            do_put(User, UserObj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[users] tried to write to a non existing user: ~p", [R]),
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
    fifo_db:put(State#state.db, <<"user">>, Key, Obj),
    riak_core_aae_vnode:update_hashtree(<<"user">>, Key, term_to_binary(Obj),
                                        State#state.hashtrees).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-ifndef(old_hash).
key_to_id(Key) ->
    [_, ID0, _] = re:split(Key, " "),
    ID1 = base64:decode(ID0),
    crypto:hash(md5,ID1).
-else.
key_to_id(Key) ->
    [_, ID0, _] = re:split(Key, " "),
    ID1 = base64:decode(ID0),
    crypto:md5(ID1).
-endif.

