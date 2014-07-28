-module(snarl_org_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("fifo_dt/include/ft.hrl").

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
-export([
         add/4,
         set/4,
         import/4,
         delete/3,
         add_trigger/4, remove_trigger/4,
         remove_target/4,
         repair/4, sync_repair/4
        ]).

-ignore_xref([
              start_vnode/1,
              handle_info/2,
              get/3,
              add/4,
              delete/3,
              add_trigger/4, remove_trigger/4,
              remove_target/4,
              set/4,
              import/4,
              repair/4, sync_repair/4
             ]).

-define(SERVICE, snarl_org).

-define(MASTER, snarl_org_vnode_master).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(Key, Obj) ->
    snarl_vnode:hash_object(Key, Obj).

aae_repair(_, Key) ->
    lager:debug("AAE Repair: ~p", [Key]),
    snarl_org:get_(Key).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Org, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Org, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Org) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Org},
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

add(Preflist, ReqID, UUID, Org) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, UUID, Org},
                                   {fsm, undefined, self()},
                                   ?MASTER).

delete(Preflist, ReqID, Org) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Org},
                                   {fsm, undefined, self()},
                                   ?MASTER).

add_trigger(Preflist, ReqID, Org, {UUID, Trigger}) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_trigger, ReqID, Org, UUID, Trigger},
                                   {fsm, undefined, self()},
                                   ?MASTER).

remove_trigger(Preflist, ReqID, Org, Val) ->
    riak_core_vnode_master:command(Preflist,
                                   {remove_trigger, ReqID, Org, Val},
                                   {fsm, undefined, self()},
                                   ?MASTER).

remove_target(Preflist, ReqID, Org, Target) ->
    riak_core_vnode_master:command(Preflist,
                                   {remove_target, ReqID, Org, Target},
                                   {fsm, undefined, self()},
                                   ?MASTER).
%%%===================================================================
%%% VNode
%%%===================================================================
init([Part]) ->
    snarl_vnode:init(Part, <<"org">>, ?SERVICE, ?MODULE, ft_org).

%%%===================================================================
%%% General
%%%===================================================================

handle_command({add, {ReqID, Coordinator} = ID, UUID, Org}, _Sender, State) ->
    Org0 = ft_org:new(ID),
    Org1 = ft_org:name(ID, Org, Org0),
    Org2 = ft_org:uuid(ID, UUID, Org1),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    OrgObj = #ft_obj{val=Org2, vclock=VC},
    snarl_vnode:put(UUID, OrgObj, State),
    {reply, {ok, ReqID}, State};

handle_command(Message, Sender, State) ->
    snarl_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"org">>, Fun, Acc0),
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
    {Org, O} = binary_to_term(Data),
    Obj = ft_obj:update(O),
    ID = snarl_vnode:mkid(handoff),
    V = ft_org:load(ID, ft_obj:val(Obj)),
    case fifo_db:get(State#vstate.db, <<"org">>, Org) of
        {ok, OldObj} ->
            V1 = ft_org:load(ID, ft_obj:val(OldObj)),
            OrgObj = #ft_obj{
                        vclock = vclock:merge([ft_obj:vclock(Obj),
                                               ft_obj:vclock(OldObj)]),
                        val = ft_org:merge(V, V1)},
            snarl_vnode:put(Org, OrgObj, State);
        not_found ->
            OrgObj = Obj#ft_obj{val=V},
            snarl_vnode:put(Org, OrgObj, State)
    end,
    {reply, ok, State}.


encode_handoff_item(Org, Data) ->
    term_to_binary({Org, Data}).

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
