-module(snarl_accounting_vnode).
-behaviour(riak_core_vnode).

%%-behaviour(riak_core_aae_vnode).

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
         %% aae_repair/2,
         hash_object/2
        ]).

%% Reads
-export([get/4, get/3]).

%% Writes
-export([
         create/4,
         delete/4,
         update/4,
         repair/3, sync_repair/4
        ]).

-ignore_xref([
              create/4,
              delete/4,
              update/4,
              repair/3, sync_repair/4
             ]).

-define(SERVICE, snarl_accounting).

-define(MASTER, snarl_accounting_vnode_master).

-record(state, {
          partition,
          node = node(),
          dbs = #{},
          db_path
         }).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(Key, Obj) ->
    snarl_vnode:hash_object(Key, Obj).

%% aae_repair(Realm, Key) ->
%%     lager:debug("AAE Repair: ~p", [Key]),
%%     snarl_org:get(Realm, Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Org, Elements) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Org, Elements},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, {Realm, Org}) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Realm, Org},
                                   {fsm, undefined, self()},
                                   ?MASTER).

get(Preflist, ReqID, {Realm, Org}, {Start, Stop}) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Realm, Org, Start, Stop},
                                   {fsm, undefined, self()},
                                   ?MASTER);

get(Preflist, ReqID, {Realm, Org}, Element) when is_binary(Element) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Realm, Org, Element},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

sync_repair(Preflist, ReqID, UUID, Obj) ->
    riak_core_vnode_master:command(Preflist,
                                   {sync_repair, ReqID, UUID, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

create(Preflist, ReqID, {Realm, OrgID}, {ElementID, Timestamp, Metadata}) ->
    riak_core_vnode_master:command(Preflist,
                                   {create, ReqID, Realm, OrgID, ElementID, Timestamp, Metadata},
                                   {fsm, undefined, self()},
                                   ?MASTER).

update(Preflist, ReqID, {Realm, OrgID}, {ElementID, Timestamp, Metadata}) ->
    riak_core_vnode_master:command(Preflist,
                                   {update, ReqID, Realm, OrgID, ElementID, Timestamp, Metadata},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, {Realm, OrgID}, {ElementID, Timestamp, Metadata}) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Realm, OrgID, ElementID, Timestamp, Metadata},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================
init([Partition]) ->
    process_flag(trap_exit, true),
    WorkerPoolSize = case application:get_env(snarl, async_workers) of
                         {ok, Val} ->
                             Val;
                         undefined ->
                             5
                     end,
    {ok, DBPath} = application:get_env(fifo_db, db_path),
    FoldWorkerPool = {pool, snarl_worker, WorkerPoolSize, []},
    {ok,
     #state{
        partition = Partition,
        db_path = DBPath
       },
     [FoldWorkerPool]}.

%%%===================================================================
%%% General
%%%===================================================================

handle_command({create, {ReqID, _}, Relam, OrgID, Element, Timestamp, Metadata},
               _Sender, State) ->
    {DB, State1} = get_db(Relam, OrgID, State),
    esqlite3:q("INSERT INTO `create` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [Element, Timestamp, term_to_binary(Metadata)], DB),
    {reply, {ok, ReqID}, State1};

handle_command({update, {ReqID, _}, Relam, OrgID, Element, Timestamp, Metadata},
               _Sender, State) ->
    {DB, State1} = get_db(Relam, OrgID, State),
    esqlite3:q("INSERT INTO `update` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [Element, Timestamp, term_to_binary(Metadata)], DB),
    {reply, {ok, ReqID}, State1};

handle_command({delete, {ReqID, _}, Relam, OrgID, Element, Timestamp, Metadata}, _Sender, State) ->
    {DB, State1} = get_db(Relam, OrgID, State),
    esqlite3:q("INSERT INTO `delete` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [Element, Timestamp, term_to_binary(Metadata)], DB),
    {reply, {ok, ReqID}, State1};

handle_command({get, ReqID, Relam, OrgID}, _Sender, State) ->
    {DB, State1} = get_db(Relam, OrgID, State),
    ResC = [ {T, create, E, binary_to_term(M)} ||
               {E, T, M} <- esqlite3:q("SELECT * FROM `create`", DB)],
    ResU = [ {T, update, E, binary_to_term(M)} ||
               {E, T, M} <- esqlite3:q("SELECT * FROM `update`", DB)],
    ResD = [ {T, delete, E, binary_to_term(M)} ||
               {E, T, M} <- esqlite3:q("SELECT * FROM `destroy`", DB)],
    Res = ResC ++ ResU ++ ResD,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State1};

handle_command({get, ReqID, Relam, OrgID, Element}, _Sender, State) ->
    {DB, State1} = get_db(Relam, OrgID, State),
    ResC = [ {T, create, E, binary_to_term(M)} ||
               {E, T, M} <- esqlite3:q("SELECT * FROM `create` "
                                       "WHERE uuid=?", [Element], DB)],
    ResU = [ {T, update, E, binary_to_term(M)} ||
               {E, T, M} <- esqlite3:q("SELECT * FROM `update` "
                                       "WHERE uuid=?", [Element], DB)],
    ResD = [ {T, delete, E, binary_to_term(M)} ||
               {E, T, M} <- esqlite3:q("SELECT * FROM `destroy` "
                                       "WHERE uuid=?", [Element], DB)],
    Res = ResC ++ ResU ++ ResD,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State1};

handle_command({get, ReqID, _Relam, _OrgID, _Start, _Stop}, _Sender, State) ->
    Res = [random:uniform(100)],
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender,
               State=#state{partition = P, db_path = Path, dbs = DBs}) ->
    [esqlite3:close(DB) || DB <- maps:values(DBs)],
    BasePath = filename:join([Path, integer_to_list(P), "accounting"]),
    AsyncWork =
        fun() ->
                case file:list_dir(BasePath) of
                    {ok, Realms} when length(Realms) > 0 ->
                        lists:foldl(
                          fun(Realm, AccR) ->
                                  RealmPath = filename:join([BasePath, Realm]),
                                  case file:list_dir(RealmPath) of
                                      {ok, Orgs} when length(Orgs) > 0 ->
                                          lists:foldl(
                                            fun(Org, AccO) ->
                                                    OrgPath = filename:join([RealmPath, Org]),
                                                    {ok, C} = esqlite3:open(OrgPath),

                                                    Fc = fun({E, T, M}, AccE) ->
                                                                 V = {T, create, E, M},
                                                                 Fun({Realm, Org}, V, AccE)
                                                         end,
                                                    Fu = fun({E, T, M}, AccE) ->
                                                                 V = {T, update, E, M},
                                                                 Fun({Realm, Org}, V, AccE)
                                                       end,
                                                    Fd = fun({E, T, M}, AccE) ->
                                                                 V = {T, delete, E, M},
                                                                 Fun({Realm, Org}, V, AccE)
                                                         end,
                                                    Cs = esqlite3:q("select * from `create`", C),
                                                    Acc1 = lists:foldl(Fc, AccO, Cs),
                                                    Us = esqlite3:q("select * from `update`", C),
                                                    Acc2 = lists:foldl(Fu, Acc1, Us),
                                                    Ds = esqlite3:q("select * from `destroy`", C),
                                                    lists:foldl(Fd, Acc2, Ds)
                                            end, AccR, Orgs);
                                      _ ->
                                          AccR
                                  end
                          end, Acc0, Realms);
                    _ ->
                        Acc0
                end
        end,
    FinishFun = fun(Acc) ->
                        riak_core_vnode:reply(Sender, Acc)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State#state{dbs = #{}}}.

handle_handoff_command(?FOLD_REQ{} = FR, Sender, State) ->
    handle_command(FR, Sender, State);

handle_handoff_command({get, _ReqID, _Vm} = Req, Sender, State) ->
    handle_command(Req, Sender, State);

handle_handoff_command(_Req, _Sender, State) ->
    {forward, State}.

handoff_starting(TargetNode, State) ->
    lager:warning("Starting handof to: ~p", [TargetNode]),
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data({{R, O}, {T, create, E, M}}, State) ->
    {DB, State1} = get_db(R, O, State),
    esqlite3:q("INSERT INTO `create` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [E, T, term_to_binary(M)], DB),
    {ok, State1};

handle_handoff_data({{R, O}, {T, update, E, M}}, State) ->
    {DB, State1} = get_db(R, O, State),
    esqlite3:q("INSERT INTO `update` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [E, T, term_to_binary(M)], DB),
    {ok, State1};

handle_handoff_data({{R, O}, {T, destroy, E, M}}, State) ->
    {DB, State1} = get_db(R, O, State),
    esqlite3:q("INSERT INTO `destroy` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [E, T, term_to_binary(M)], DB),
    {ok, State1}.

encode_handoff_item(Org, Data) ->
    term_to_binary({Org, Data}).

is_empty(State  = #state{partition = P, db_path = Path}) ->
    BasePath = filename:join([Path, integer_to_list(P), "accounting"]),
    case file:list_dir(BasePath) of
        {ok, [_ | _]} ->
            {false, State};
        _ ->
            {true, State}
    end.

delete(State = #state{partition = P, db_path = Path, dbs = DBs}) ->
    [esqlite3:close(DB) || DB <- maps:values(DBs)],
    BasePath = filename:join([Path, integer_to_list(P), "accounting"]),
    del_dir(BasePath),
    {ok, State#state{dbs = #{}}}.

handle_coverage(ReqID, _KeySpaces, _Sender, State) ->
    %% TODO: snarl_vnode:handle_coverage(Req, KeySpaces, Sender, State).
    %% We don't really have coverage
    {reply, {ok, ReqID}, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{dbs = DBs}) ->
    [esqlite3:close(DB) || DB <- maps:values(DBs)],
    ok.

handle_info(_Msg, State) ->
    %% TODO: snarl_vnode:handle_info(Msg, State).
    %% we do't have infos.
    {ok, State}.

get_db(Realm, Org, State = #state{partition = P, dbs = DBs, db_path = Path}) ->
    Key = {Realm, Org},
    case maps:find(Key, DBs) of
        {ok, DB} ->
            {DB, State};
        error ->
            Realms = binary_to_list(Realm),
            Orgs = binary_to_list(Org),
            BasePath = filename:join([Path, integer_to_list(P),
                                      "accounting", Realms]),
            file:make_dir(filename:join([Path])),
            file:make_dir(filename:join([Path, integer_to_list(P)])),
            file:make_dir(filename:join([Path, integer_to_list(P),
                                         "accounting"])),
            file:make_dir(filename:join([Path, integer_to_list(P),
                                         "accounting", Realms])),
            DBFile = filename:join([BasePath, Orgs]),
            {ok, DB} = esqlite3:open(DBFile),
            init_db(DB),
            DBs1 = maps:put(Key, DB, DBs),
            {DB, State#state{dbs = DBs1}}
    end.

init_db(DB) ->
    ok = esqlite3:exec("CREATE TABLE IF NOT EXISTS `create` "
                       "(uuid CHAR(36), time INTEGER, metadata BLOB)", DB),
    ok = esqlite3:exec("CREATE TABLE IF NOT EXISTS `destroy` "
                       "(uuid CHAR(36), time INTEGER, metadata BLOB)", DB),
    ok = esqlite3:exec("CREATE TABLE IF NOT EXISTS `update` "
                       "(uuid CHAR(36), time INTEGER, metadata BLOB)", DB).


del_dir(Dir) ->
    lists:foreach(fun(D) ->
                          ok = file:del_dir(D)
                  end, del_all_files([Dir], [])).

del_all_files([], EmptyDirs) ->
    EmptyDirs;
del_all_files([Dir | T], EmptyDirs) ->
    {ok, FilesInDir} = file:list_dir(Dir),
    {Files, Dirs} = lists:foldl(fun(F, {Fs, Ds}) ->
                                        Path = Dir ++ "/" ++ F,
                                        case filelib:is_dir(Path) of
                                            true ->
                                                {Fs, [Path | Ds]};
                                            false ->
                                                {[Path | Fs], Ds}
                                        end
                                end, {[],[]}, FilesInDir),
    lists:foreach(fun(F) ->
                          ok = file:delete(F)
                  end, Files),
    del_all_files(T ++ Dirs, [Dir | EmptyDirs]).
