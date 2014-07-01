-module(snarl_vnode).

-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([init/5,
         is_empty/1,
         delete/1,
         put/3,
         change/5,
         fold/4,
         handle_command/3,
         handle_coverage/4,
         handle_info/2,
         mkid/0,
         mkid/1,
         hash_object/2,
         mk_reqid/0]).

-ignore_xref([change/5, mkid/0]).

hash_object(Key, Obj) ->
    Obj1 = lists:sort(Obj),
    Hash = term_to_binary(erlang:phash2({Key, Obj1})),
    lager:debug("Hashing Key: ~p + ~p -> ~p", [Key, Obj1, Hash]),
    Hash.

mkid() ->
    mkid(node()).

mkid(Actor) ->
    {mk_reqid(), Actor}.

mk_reqid() ->
    {MegaSecs,Secs,MicroSecs} = erlang:now(),
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

init(Partition, Bucket, Service, VNode, StateMod) ->
    DB = list_to_atom(integer_to_list(Partition)),
    fifo_db:start(DB),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(Service, Partition, VNode,
                                                    undefined),
    WorkerPoolSize = case application:get_env(snarl, async_workers) of
                         {ok, Val} ->
                             Val;
                         undefined ->
                             5
                     end,
    FoldWorkerPool = {pool, snarl_worker, WorkerPoolSize, []},
    {ok,
     #vstate{db=DB, hashtrees=HT, partition=Partition, node=node(),
             service_bin=list_to_binary(atom_to_list(Service)),
             service=Service, bucket=Bucket, state=StateMod, vnode=VNode},
     [FoldWorkerPool]}.

list_keys(Sender, State=#vstate{db=DB, bucket=Bucket}) ->
    FoldFn = fun (K, L) ->
                     [K|L]
             end,
    AsyncWork = fun() ->
                        fifo_db:fold_keys(DB, Bucket, FoldFn, [])
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State}.

list_keys(Getter, Requirements, Sender, State=#vstate{state=StateMod}) ->
    ID = snarl_vnode:mkid(list),
    FoldFn = fun (Key, E, C) ->
                     E1 = E#snarl_obj{val=StateMod:load(ID, E#snarl_obj.val)},
                     case rankmatcher:match(E1, Getter, Requirements) of
                         false ->
                             C;
                         Pts ->
                             [{Pts, Key} | C]
                     end
             end,
    fold(FoldFn, [], Sender, State).

list(Getter, Requirements, Sender, State=#vstate{state=StateMod}) ->
    ID = mkid(list),
    FoldFn = fun (Key, E, C) ->
                     E1 = E#snarl_obj{val=StateMod:load(ID, E#snarl_obj.val)},
                     case rankmatcher:match(E1, Getter, Requirements) of
                         false ->
                             C;
                         Pts ->
                             [{Pts, {Key, E1}} | C]
                     end
             end,
    fold(FoldFn, [], Sender, State).

fold(Fun, Acc0, Sender, State=#vstate{db=DB, bucket=Bucket}) ->
    AsyncWork = fun() ->
                        fifo_db:fold(DB, Bucket, Fun, Acc0)
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State}.

put(Key, Obj, State) ->
    fifo_db:put(State#vstate.db, State#vstate.bucket, Key, Obj),
    snarl_sync_tree:update(State#vstate.service, Key, Obj),
    riak_core_aae_vnode:update_hashtree(
      State#vstate.service_bin, Key, Obj#snarl_obj.vclock, State#vstate.hashtrees).

change(UUID, Action, Vals, {ReqID, Coordinator} = ID,
       State=#vstate{state=Mod}) ->
    case fifo_db:get(State#vstate.db, State#vstate.bucket, UUID) of
        {ok, #snarl_obj{val=H0} = O} ->
            H1 = case Mod:load(ID, H0) of
                     _H when _H =:= H0 ->
                         H0;
                     Hx ->
                         O1 = O#snarl_obj{val=Hx},
                         fifo_db:put(State#vstate.db, State#vstate.bucket, UUID, O1),
                         Hx
                 end,
            H2 = case Vals of
                     [Val] ->
                         Mod:Action(ID, Val, H1);
                     [Val1, Val2] ->
                         Mod:Action(ID, Val1, Val2, H1)
                 end,
            Obj = snarl_obj:update(H2, Coordinator, O),
            snarl_vnode:put(UUID, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[~s] tried to write to a non existing element: ~p => ~p",
                        [State#vstate.bucket, UUID, R]),
            {reply, {ok, ReqID, not_found}, State}
    end.

%%%===================================================================
%%% Callbacks
%%%===================================================================

is_empty(State=#vstate{db=DB, bucket=Bucket}) ->
    FoldFn = fun (_, _) -> {false, State} end,
    fifo_db:fold_keys(DB, Bucket, FoldFn, {true, State}).

delete(State=#vstate{db=DB, bucket=Bucket}) ->
    FoldFn = fun (K, A) -> [{delete, <<Bucket/binary, K/binary>>} | A] end,
    Trans = fifo_db:fold_keys(DB, Bucket, FoldFn, []),
    fifo_db:transact(State#vstate.db, Trans),
    {ok, State}.

load_obj(ID, Mod, Obj = #snarl_obj{val = V}) ->
    Obj#snarl_obj{val = Mod:load(ID, V)}.

handle_coverage({wipe, UUID}, _KeySpaces, {_, ReqID, _}, State) ->
    fifo_db:delete(State#vstate.db, State#vstate.bucket, UUID),
    {reply, {ok, ReqID}, State};

handle_coverage({lookup, Name}, _KeySpaces, Sender, State=#vstate{state=Mod}) ->
    ID = mkid(lookup),
    FoldFn = fun (U, #snarl_obj{val=V}, [not_found]) ->
                     case Mod:name(Mod:load(ID, V)) of
                         AName when AName =:= Name ->
                             [U];
                         _ ->
                             [not_found]
                     end;
                 (_, _O, Res) ->
                     Res
             end,
    fold(FoldFn, [not_found], Sender, State);

handle_coverage(list, _KeySpaces, Sender, State) ->
    list_keys(Sender, State);

handle_coverage({list, Requirements}, _KeySpaces, Sender, State) ->
    handle_coverage({list, Requirements, false}, _KeySpaces, Sender, State);

handle_coverage({list, Requirements, Full}, _KeySpaces, Sender,
                State = #vstate{state=Mod}) ->
    case Full of
        true ->
            list(fun Mod:getter/2, Requirements, Sender, State);
        false ->
            list_keys(fun Mod:getter/2, Requirements, Sender, State)
    end;

handle_coverage(Req, _KeySpaces, _Sender, State) ->
    lager:warning("Unknown coverage request: ~p", [Req]),
    {stop, not_implemented, State}.

handle_command({sync_repair, {ReqID, _}, UUID, Obj = #snarl_obj{}}, _Sender,
               State=#vstate{state=Mod}) ->
    case get(UUID, State) of
        {ok, Old} ->
            ID = snarl_vnode:mkid(),
            Old1 = load_obj(ID, Mod, Old),
            lager:info("[sync-repair:~s] Merging with old object", [UUID]),
            Merged = snarl_obj:merge(snarl_entity_read_fsm, [Old1, Obj]),
            snarl_vnode:put(UUID, Merged, State);
        not_found ->
            lager:info("[sync-repair:~s] Writing new object", [UUID]),
            snarl_vnode:put(UUID, Obj, State);
        _ ->
            lager:error("[~s] Read repair failed, data was updated too recent.",
                        [State#vstate.bucket])
    end,
    {reply, {ok, ReqID}, State};

handle_command({repair, UUID, _VClock, Obj = #snarl_obj{}}, _Sender,
               State=#vstate{state=Mod}) ->
    case get(UUID, State) of
        {ok, Old} ->
            ID = snarl_vnode:mkid(),
            Old1 = load_obj(ID, Mod, Old),
            Merged = snarl_obj:merge(snarl_entity_read_fsm, [Old1, Obj]),
            snarl_vnode:put(UUID, Merged, State);
        not_found ->
            snarl_vnode:put(UUID, Obj, State);
        _ ->
            lager:error("[~s] Read repair failed, data was updated too recent.",
                        [State#vstate.bucket])
    end,
    {noreply, State};

handle_command({get, ReqID, UUID}, _Sender, State=#vstate{state=Mod}) ->
    Res = case fifo_db:get(State#vstate.db, State#vstate.bucket, UUID) of
              {ok, #snarl_obj{val = V0} = O} ->
                  ID = {ReqID, load},
                  case Mod:load(ID, V0) of
                      _V when _V =:= V0 ->
                          O;
                      Hx ->
                          O1 = O#snarl_obj{val=Hx},
                          fifo_db:put(State#vstate.db, State#vstate.bucket, UUID, O1),
                          O1
                  end;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#vstate.partition, State#vstate.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({delete, {ReqID, _Coordinator}, UUID}, _Sender, State) ->
    fifo_db:delete(State#vstate.db, State#vstate.bucket, UUID),
    riak_core_index_hashtree:delete(
      {State#vstate.service_bin, UUID}, State#vstate.hashtrees),
    {reply, {ok, ReqID}, State};

handle_command({set, {ReqID, Coordinator}=ID, UUID, Attributes}, _Sender,
               State=#vstate{state=Mod, bucket=Bucket}) ->
    case fifo_db:get(State#vstate.db, Bucket, UUID) of
        {ok, #snarl_obj{val=H0} = O} ->
            H1 = Mod:load(ID, H0),
            H2 = lists:foldr(
                   fun ({Attribute, Value}, H) ->
                           Mod:set_metadata(ID, Attribute, Value, H)
                   end, H1, Attributes),
            Obj = snarl_obj:update(H2, Coordinator, O),
            snarl_vnode:put(UUID, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[~s] tried to write to a non existing uuid: ~p",
                        [Bucket, R]),
            {reply, {ok, ReqID, not_found}, State}
    end;

handle_command({import, {ReqID, Coordinator} = ID, UUID, Data}, _Sender,
               State=#vstate{state=Mod}) ->
    H1 = Mod:load(ID, Data),
    H2 = Mod:uuid(ID, UUID, H1),
    case fifo_db:get(State#vstate.db, State#vstate.bucket, UUID) of
        {ok, O} ->
            Obj = snarl_obj:update(H2, Coordinator, O),
            snarl_vnode:put(UUID, Obj, State),
            {reply, {ok, ReqID}, State};
        _R ->
            VC0 = vclock:fresh(),
            VC = vclock:increment(Coordinator, VC0),
            Obj = #snarl_obj{val=H2, vclock=VC},
            snarl_vnode:put(UUID, Obj, State),
            {reply, {ok, ReqID}, State}
    end;

%%%===================================================================
%%% AAE
%%%===================================================================

handle_command({hashtree_pid, Node}, _, State) ->
    %% Handle riak_core request forwarding during ownership handoff.
    %% Following is necessary in cases where anti-entropy was enabled
    %% after the vnode was already running
    case {node(), State#vstate.hashtrees} of
        {Node, undefined} ->
            HT1 =  riak_core_aae_vnode:maybe_create_hashtrees(
                     State#vstate.service,
                     State#vstate.partition,
                     State#vstate.vnode,
                     undefined),
            {reply, {ok, HT1}, State#vstate{hashtrees = HT1}};
        {Node, HT} ->
            {reply, {ok, HT}, State};
        _ ->
            {reply, {error, wrong_node}, State}
    end;

handle_command({rehash, {_, UUID}}, _,
               State=#vstate{service_bin=ServiceBin, hashtrees=HT}) ->
    case get(UUID, State) of
        {ok, Obj} ->
            riak_core_aae_vnode:update_hashtree(
              ServiceBin, UUID, Obj#snarl_obj.vclock, HT);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete({ServiceBin, UUID}, HT)
    end,
    {noreply, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender, State) ->
    FoldFn = fun(K, V, O) ->
                     Fun({State#vstate.service_bin, K}, V, O)
             end,
    fold(FoldFn, Acc0, Sender, State);

handle_command({Action, ID, UUID, Param1, Param2}, _Sender, State) ->
    change(UUID, Action, [Param1, Param2], ID, State);

handle_command({Action, ID, UUID, Param}, _Sender, State) ->
    change(UUID, Action, [Param], ID, State);

handle_command(Message, _Sender, State) ->
    lager:error("[~s] Unknown command: ~p", [State#vstate.bucket, Message]),
    {noreply, State}.

reply(Reply, {_, ReqID, _} = Sender, #vstate{node=N, partition=P}) ->
    riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, Reply}).

get(UUID, State) ->
    fifo_db:get(State#vstate.db, State#vstate.bucket, UUID).

handle_info(retry_create_hashtree,
            State=#vstate{service=Srv, hashtrees=undefined, partition=Idx,
                          vnode=VNode}) ->
    lager:debug("~p/~p retrying to create a hash tree.", [Srv, Idx]),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(Srv, Idx, VNode, undefined),
    {ok, State#vstate{hashtrees = HT}};
handle_info(retry_create_hashtree, State) ->
    {ok, State};
handle_info({'DOWN', _, _, Pid, _},
            State=#vstate{service=Service, hashtrees=Pid, partition=Idx}) ->
    lager:debug("~p/~p hashtree ~p went down.", [Service, Idx, Pid]),
    erlang:send_after(1000, self(), retry_create_hashtree),
    {ok, State#vstate{hashtrees = undefined}};
handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};
handle_info(_, State) ->
    {ok, State}.
