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
    {ok, ReqID} = snarl_group_read_fsm:get(Group),
    wait_for_reqid(ReqID, ?TIMEOUT).

list() ->
    {ok, ReqID} = snarl_group_read_fsm:list(),
    wait_for_reqid(ReqID, ?TIMEOUT).

add(Group) ->
    {ok, ReqID} = snarl_group_read_fsm:get(Group),
    case wait_for_reqid(ReqID, ?TIMEOUT) of
	{ok, not_found} ->
	    do_update(Group, add);
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
    {ok, ReqID} = snarl_group_read_fsm:get(Group),
    case wait_for_reqid(ReqID, ?TIMEOUT) of
	{ok, not_found} ->
	    not_found;
	{ok, _GroupObj} ->
	    do_write(Group, Op)
    end.

do_update(Group, Op, Val) ->
    {ok, ReqID} = snarl_group_read_fsm:get(Group),
    case wait_for_reqid(ReqID, ?TIMEOUT) of
	{ok, not_found} ->
	    not_found;
	{ok, _GroupObj} ->
	    do_write(Group, Op, Val)
    end.

do_write(Group, Op) ->
    {ok, ReqID} = snarl_group_write_fsm:write(Group, Op),
    wait_for_reqid(ReqID, ?TIMEOUT).

do_write(Group, Op, Val) ->
    {ok, ReqID} = snarl_group_write_fsm:write(Group, Op, Val),
    wait_for_reqid(ReqID, ?TIMEOUT).

wait_for_reqid(ReqID, Timeout) ->
    ?PRINT({waiting_for, ReqID}),
    receive
	{ReqID, ok} -> 
	    ok;
        {ReqID, ok, Val} -> 
	    {ok, Val};
	Other -> 
	    ?PRINT({yuck, Other})
    after Timeout ->
	    {error, timeout}
    end.
