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
         gc/4,
         grant/4, revoke/4, revoke_prefix/4,
         join/4, leave/4,
         passwd/4,
         join_org/4, leave_org/4, select_org/4,
         revoke_key/4,
         set/4
        ]).

-ignore_xref([
              add/4,
              add_key/4,
              find_key/3,
              delete/3,
              gc/4,
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
              join_org/4, leave_org/4, select_org/4,
              start_vnode/1
             ]).


-record(state, {db, partition, node}).

-define(MASTER, snarl_user_vnode_master).

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
%%%===================================================================

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

gc(Preflist, ReqID, UUID, GCable) ->
    riak_core_vnode_master:command(Preflist,
                                   {gc, ReqID, UUID, GCable},
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
    snarl_db:start(DB),
    {ok, #state {db = DB, node = node(), partition = Partition}}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, User, _VClock, #snarl_obj{val = V} = Obj},
               _Sender, State) ->
    case snarl_db:get(State#state.db, <<"user">>, User) of
        {ok, #snarl_obj{val = V0}} ->
            V1 = snarl_user_state:load(V0),
            snarl_db:put(State#state.db, <<"user">>, User,
                         Obj#snarl_obj{val = snarl_user_state:merge(V, V1)});
        not_found ->
            snarl_db:put(State#state.db, <<"user">>, User, Obj)
    end,
    {noreply, State};


handle_command({get, ReqID, User}, _Sender, State) ->
    Res = case snarl_db:get(State#state.db, <<"user">>, User) of
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
    snarl_db:put(State#state.db, <<"user">>, UUID, UserObj),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, User}, _Sender, State) ->
    snarl_db:delete(State#state.db, <<"user">>, User),
    {reply, {ok, ReqID}, State};

handle_command({join = Action, {ReqID, Coordinator}, User, Group}, _Sender, State) ->
    case snarl_group:get(Group) of
        not_found ->
            {reply, {ok, ReqID, not_found}, State};
        {ok, _GroupObj} ->
            change_user(User, Action, [Group], Coordinator, State, ReqID)
    end;

handle_command({set, {ReqID, Coordinator}, User, Attributes}, _Sender, State) ->
    case snarl_db:get(State#state.db, <<"user">>, User) of
        {ok, #snarl_obj{val=H0} = O} ->
            H1 = snarl_user_state:load(H0),
            H2 = lists:foldr(
                   fun ({Attribute, Value}, H) ->
                           snarl_user_state:set_metadata(Attribute, Value, H)
                   end, H1, Attributes),
            H3 = snarl_user_state:expire(?STATEBOX_EXPIRE, H2),
            snarl_db:put(State#state.db, <<"user">>, User,
                         snarl_obj:update(H3, Coordinator, O)),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[users] tried to write to a non existing user: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({import, {ReqID, Coordinator} = ID, UUID, Data}, _Sender, State) ->
    H1 = snarl_user_state:load(Data),
    H2 = snarl_user_state:uuid(ID, UUID, H1),
    case snarl_db:get(State#state.db, <<"user">>, UUID) of
        {ok, O} ->
            snarl_db:put(State#state.db, <<"user">>, UUID,
                         snarl_obj:update(H2, Coordinator, O)),
            {reply, {ok, ReqID}, State};
        _R ->
            VC0 = vclock:fresh(),
            VC = vclock:increment(Coordinator, VC0),
            UserObj = #snarl_obj{val=H2, vclock=VC},
            snarl_db:put(State#state.db, <<"user">>, UUID, UserObj),
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
    Acc = snarl_db:fold(State#state.db, <<"user">>, Fun, Acc0),
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
    case snarl_db:get(State#state.db, <<"user">>, User) of
        {ok, #snarl_obj{val = V0}} ->
            V1 = snarl_user_state:load(V0),
            snarl_db:put(State#state.db, <<"user">>, User,
                         Obj#snarl_obj{val = snarl_user_state:merge(V, V1)});
        not_found ->
            VC0 = vclock:fresh(),
            VC = vclock:increment(node(), VC0),
            UserObj = #snarl_obj{val=V, vclock=VC},
            snarl_db:put(State#state.db, <<"user">>, User, UserObj)
    end,
    {reply, ok, State}.

encode_handoff_item(User, Data) ->
    term_to_binary({User, Data}).

is_empty(State) ->
    snarl_db:fold(State#state.db,
                  <<"user">>,
                  fun (_,_, _) ->
                          {false, State}
                  end, {true, State}).

delete(State) ->
    Trans = snarl_db:fold(State#state.db,
                          <<"user">>,
                          fun (K,_, A) ->
                                  [{delete, <<"user", K/binary>>} | A]
                          end, []),
    snarl_db:transact(State#state.db, Trans),
    {ok, State}.

handle_coverage({find_key, KeyID}, _KeySpaces, {_, ReqID, _}, State) ->
    Res = snarl_db:fold(State#state.db,
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
    Res = snarl_db:fold(State#state.db,
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
    List = snarl_db:fold(State#state.db,
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
    List = snarl_db:fold(State#state.db,
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
    case snarl_db:get(State#state.db, <<"user">>, User) of
        {ok, #snarl_obj{val=H0} = O} ->
            H1 = snarl_user_state:load(H0),
            ID = {ReqID, Coordinator},
            H2 = case Vals of
                     [Val] ->
                         snarl_user_state:Action(ID, Val, H1);
                     [Val1, Val2] ->
                         snarl_user_state:Action(ID, Val1, Val2, H1)
                 end,
            snarl_db:put(State#state.db, <<"user">>, User,
                         snarl_obj:update(H2, Coordinator, O)),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[users] tried to write to a non existing user: ~p", [R]),
            {reply, {ok, ReqID, not_found}, State}
    end.
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
