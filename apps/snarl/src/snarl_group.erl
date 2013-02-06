-module(snarl_group).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
         list/0,
         get/1,
         lookup/1,
         add/1,
         delete/1,
         grant/2,
         revoke/2,
         set/2,
         set/3
        ]).

-ignore_xref([ping/0]).

-define(TIMEOUT, 5000).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, snarl_group),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, snarl_group_vnode_master).

lookup(Group) ->
    {ok, Res} = snarl_entity_coverage_fsm:start(
                  {snarl_group_vnode, snarl_group},
                  lookup, Group
                 ),
    Res1 = lists:foldl(fun (not_found, Acc) ->
                               Acc;
                           (R, _) ->
                               R
                       end, not_found, Res),
    {ok, Res1}.

-spec get(Group::fifo:group_id()) ->
                 {ok, fifo:group()} |
                 not_found |
                 {error, timeout}.
get(Group) ->
    snarl_entity_read_fsm:start(
      {snarl_group_vnode, snarl_group},
      get, Group
     ).

-spec list() -> {ok, [fifo:group_id()]} |
                not_found |
                {error, timeout}.

list() ->
    snarl_entity_coverage_fsm:start(
      {snarl_group_vnode, snarl_group},
      list
     ).

-spec add(Group::fifo:group_id()) ->
                 {ok, binary()} |
                 douplicate |
                 {error, timeout}.

add(Group) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    case snarl_group:lookup(Group) of
        {ok, not_found} ->
            ok = do_write(UUID, add, Group),
            {ok, UUID};
        {ok, _GroupObj} ->
            duplicate
    end.

-spec delete(Group::fifo:group_id()) ->
                    ok |
                    not_found|
                    {error, timeout}.

delete(Group) ->
    do_write(Group, delete).

-spec grant(Group::fifo:group_id(), fifo:permission()) ->
                   ok |
                   not_found|
                   {error, timeout}.

grant(Group, Permission) ->
    do_write(Group, grant, Permission).

-spec revoke(Group::fifo:group_id(), fifo:permission()) ->
                    ok |
                    not_found|
                    {error, timeout}.

revoke(Group, Permission) ->
    do_write(Group, revoke, Permission).

set(Group, Attribute, Value) ->
    set(Group, set, [{Attribute, Value}]).

set(Group, Attributes) ->
    do_write(Group, set, Attributes).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Group, Op) ->
    snarl_entity_write_fsm:write({snarl_group_vnode, snarl_group}, Group, Op).

do_write(Group, Op, Val) ->
    snarl_entity_write_fsm:write({snarl_group_vnode, snarl_group}, Group, Op, Val).
