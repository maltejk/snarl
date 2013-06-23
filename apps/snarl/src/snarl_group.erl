-module(snarl_group).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
         list/0,
         get/1,
         get_/1,
         lookup/1,
         add/1,
         delete/1,
         grant/2,
         revoke/2,
         set/2,
         set/3,
         create/2,
         revoke_prefix/2
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

-spec lookup(GroupName::binary()) ->
                    not_found |
                    {error, timeout} |
                    {ok, Group::fifo:group()}.

lookup(GroupName) ->
    {ok, Res} = snarl_entity_coverage_fsm:start(
                  {snarl_group_vnode, snarl_group},
                  lookup, GroupName),
    R0 = lists:foldl(fun (not_found, Acc) ->
                             Acc;
                         (R, _) ->
                             {ok, R}
                     end, not_found, Res),
    case R0 of
        {ok, UUID} ->
            snarl_group:get(UUID);
        R ->
            R
    end.

-spec get(Group::fifo:group_id()) ->
                 not_found |
                 {error, timeout} |
                 {ok, Group::fifo:group()}.
get(Group) ->
    case get_(Group) of
        {ok, GroupObj} ->
            {ok, snarl_group_state:to_json(GroupObj)};
        R  ->
            R
    end.

-spec get_(Group::fifo:group_id()) ->
                 not_found |
                 {error, timeout} |
                 {ok, Group::#?GROUP{}}.
get_(Group) ->
    case snarl_entity_read_fsm:start(
           {snarl_group_vnode, snarl_group},
           get, Group
          ) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

-spec list() -> {ok, [fifo:group_id()]} |
                not_found |
                {error, timeout}.

list() ->
    snarl_entity_coverage_fsm:start(
      {snarl_group_vnode, snarl_group},
      list
     ).

-spec add(Group::binary()) ->
                 {ok, UUID::fifo:group_id()} |
                 douplicate |
                 {error, timeout}.

add(Group) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    create(UUID, Group).

create(UUID, Group) ->
    case snarl_group:lookup(Group) of
        not_found ->
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

-spec revoke_prefix(Group::fifo:group_id(), fifo:permission()) ->
                           ok |
                           not_found|
                           {error, timeout}.

revoke_prefix(Group, Prefix) ->
    do_write(Group, revoke_prefix, Prefix).

-spec set(Group::fifo:group_id(), Attirbute::fifo:key(), Value::fifo:value()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(Group, Attribute, Value) ->
    set(Group, [{Attribute, Value}]).

-spec set(Group::fifo:group_id(), Attirbutes::fifo:attr_list()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(Group, Attributes) ->
    do_write(Group, set, Attributes).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Group, Op) ->
    snarl_entity_write_fsm:write({snarl_group_vnode, snarl_group}, Group, Op).

do_write(Group, Op, Val) ->
    snarl_entity_write_fsm:write({snarl_group_vnode, snarl_group}, Group, Op, Val).
