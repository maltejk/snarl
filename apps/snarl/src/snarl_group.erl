-module(snarl_group).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
	 list/0,
	 get/1,
	 add/1,
	 delete/1,
	 grant/2,
	 revoke/2
        ]).

-define(TIMEOUT, 5000).


%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, snarl_group),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, snarl_group_vnode_master).

get(Group) ->
    snarl_entity_read_fsm:start(
      {snarl_group_vnode, snarl_group},
      get, Group
     ).

list() ->
    snarl_entity_coverage_fsm:start(
      {snarl_group_vnode, snarl_group},
      list
     ).

add(Group) ->
    case snarl_group:get(Group) of
	{ok, not_found} ->
	    do_write(Group, add);
	{ok, _GroupObj} ->
	    duplicate
    end.

delete(Group) ->
    do_update(Group, delete).

grant(Group, Permission) ->
    do_update(Group, grant, Permission).

revoke(Group, Permission) ->
    do_update(Group, revoke, Permission).



%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_update(Group, Op) ->
    case snarl_group:get(Group) of
	{ok, not_found} ->
	    not_found;
	{ok, _GroupObj} ->
	    do_write(Group, Op)
    end.

do_update(Group, Op, Val) ->
    case snarl_group:get(Group) of
	{ok, not_found} ->
	    not_found;
	{ok, _GroupObj} ->
	    do_write(Group, Op, Val)
    end.

do_write(Group, Op) ->
    snarl_entity_write_fsm:write({snarl_group_vnode, snarl_group}, Group, Op).

do_write(Group, Op, Val) ->
    snarl_entity_write_fsm:write({snarl_group_vnode, snarl_group}, Group, Op, Val).
