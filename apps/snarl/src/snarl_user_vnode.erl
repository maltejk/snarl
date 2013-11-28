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
         handle_exit/3,
         handle_info/2
       ]).

-export([
         master/0,
         aae_repair/2,
         hash_object/2
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


-define(SERVICE, snarl_user).

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
init([Part]) ->
    snarl_vnode:init(Part, <<"user">>, ?SERVICE, ?MODULE, snarl_user_state).

%%%===================================================================
%%% General
%%%===================================================================

handle_command({add, {ReqID, Coordinator}=ID, UUID, User}, _Sender, State) ->
    User0 = snarl_user_state:new(ID),
    User1 = snarl_user_state:name(ID, User, User0),
    User2 = snarl_user_state:uuid(ID, UUID, User1),
    User3 = snarl_user_state:grant(ID, [<<"users">>, UUID, <<"...">>], User2),
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    UserObj = #snarl_obj{val=User3, vclock=VC},
    snarl_vnode:put(UUID, UserObj, State),
    {reply, {ok, ReqID}, State};

handle_command({join = Action, {ReqID, _}=ID, User, Group}, _Sender, State) ->
    case snarl_group:get(Group) of
        not_found ->
            {reply, {ok, ReqID, not_found}, State};
        {ok, _GroupObj} ->
            snarl_vnode:change(User, Action, [Group], ID, State)
    end;

handle_command(Message, Sender, State) ->
    snarl_vnode:handle_command(Message, Sender, State).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = fifo_db:fold(State#vstate.db, <<"user">>, Fun, Acc0),
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
    ID = snarl_vnode:mkid(handoff),
    V = snarl_user_state:load(ID, Vin),
    case fifo_db:get(State#vstate.db, <<"user">>, User) of
        {ok, #snarl_obj{val = V0}} ->
            V1 = snarl_user_state:load(ID, V0),
            UserObj = Obj#snarl_obj{val = snarl_user_state:merge(V, V1)},
            snarl_vnode:put(User, UserObj, State);
        not_found ->
            VC0 = vclock:fresh(),
            VC = vclock:increment(node(), VC0),
            UserObj = #snarl_obj{val=V, vclock=VC},
            snarl_vnode:put(User, UserObj, State)
    end,
    {reply, ok, State}.

encode_handoff_item(User, Data) ->
    term_to_binary({User, Data}).

is_empty(State) ->
    snarl_vnode:is_empty(State).

delete(State) ->
    snarl_vnode:delete(State).

handle_coverage({find_key, KeyID}, _KeySpaces, Sender, State) ->
    ID = snarl_vnode:mkid(findkey),
    FoldFn = fun (UUID, #snarl_obj{val=U0}, [not_found]) ->
                     U1 = snarl_user_state:load(ID, U0),
                     Ks = snarl_user_state:keys(U1),
                     Ks1 = [key_to_id(K) || {_, K} <- Ks],
                     case lists:member(KeyID, Ks1) of
                         true ->
                             [UUID];
                         _ ->
                             [not_found]
                     end;
                 (_U, _, Res) ->
                     Res
             end,
    snarl_vnode:fold(FoldFn, [not_found], Sender, State);

handle_coverage({lookup, Name}, _KeySpaces, Sender, State) ->
    snarl_vnode:lookup(Name, Sender, State);

handle_coverage(list, _KeySpaces, Sender, State) ->
    snarl_vnode:list_keys(Sender, State);

handle_coverage({list, Requirements}, _KeySpaces, Sender, State) ->
    ID = snarl_vnode:mkid(findkey),
    Getter = fun(#snarl_obj{val=S0}, <<"uuid">>) ->
                     snarl_user_state:uuid(snarl_user_state:load(ID, S0))
             end,
    snarl_vnode:list_keys(Getter, Requirements, Sender, State);

handle_coverage(Req, _KeySpaces, _Sender, State) ->
    lager:warning("Unknown coverage request: ~p", [Req]),
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
handle_info(Msg, State) ->
    snarl_vnode:handle_info(Msg, State).

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

