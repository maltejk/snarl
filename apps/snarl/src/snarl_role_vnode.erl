-module(snarl_role_vnode).
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
         hash_object/2
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
         repair/4, sync_repair/4,
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
              repair/4, sync_repair/4,
              revoke/4,
              revoke_prefix/4,
              handle_info/2
             ]).

-define(SERVICE, snarl_role).

-define(MASTER, snarl_role_vnode_master).

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
    snarl_role:get_(Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


repair(IdxNode, Role, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Role, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Role) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Role},
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

add(Preflist, ReqID, UUID, Role) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, UUID, Role},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Role) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Role},
                                   {fsm, undefined, self()},
                                   ?MASTER).

grant(Preflist, ReqID, Role, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {grant, ReqID, Role, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

revoke(Preflist, ReqID, Role, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {revoke, ReqID, Role, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

revoke_prefix(Preflist, ReqID, Role, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {revoke_prefix, ReqID, Role, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).
%%%===================================================================
%%% VNode
%%%===================================================================
init([Part]) ->
    snarl_vnode:init(Part, <<"role">>, ?SERVICE, ?MODULE, snarl_role_state).

%%%===================================================================
%%% General
%%%===================================================================

handle_command({add, {ReqID, Coordinator} = ID, UUID, Role}, _Sender, State) ->
    Role0 = snarl_role_state:new(ID),
    Role1 = snarl_role_state:name(ID, Role, Role0),
    Role2 = snarl_role_state:uuid(ID, UUID, Role1),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    RoleObj = #snarl_obj{val=Role2, vclock=VC},
    snarl_vnode:put(UUID, RoleObj, State),
    {reply, {ok, ReqID}, State};

handle_command(Message, Sender, State) ->
    snarl_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"role">>, Fun, Acc0),
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
    {Role, #snarl_obj{val = Vin} = Obj} = binary_to_term(Data),
    ID = snarl_vnode:mkid(handoff),
    V = snarl_role_state:load(ID, Vin),
    case fifo_db:get(State#vstate.db, <<"role">>, Role) of
        {ok, #snarl_obj{val = V0}} ->
            V1 = snarl_role_state:load(ID, V0),
            RoleObj = Obj#snarl_obj{val = snarl_role_state:merge(V, V1)},
            snarl_vnode:put(Role, RoleObj, State);
        not_found ->
            VC0 = vclock:fresh(),
            VC = vclock:increment(node(), VC0),
            RoleObj = #snarl_obj{val=V, vclock=VC},
            snarl_vnode:put(Role, RoleObj, State)
    end,
    {reply, ok, State}.

encode_handoff_item(Role, Data) ->
    term_to_binary({Role, Data}).

is_empty(State) ->
    snarl_vnode:is_empty(State).

delete(State) ->
    snarl_vnode:delete(State).

handle_coverage(Req, KeySpaces, Sender, State) ->
    snarl_vnode:handle_coverage(Req, KeySpaces, Sender, State).

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(Msg, State) ->
    snarl_vnode:handle_info(Msg, State).
