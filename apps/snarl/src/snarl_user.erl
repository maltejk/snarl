-module(snarl_user).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         ping/0,
         list/0,
         auth/2,
         get/1,
         lookup/1,
         add/1,
         delete/1,
         passwd/2,
         join/2,
         leave/2,
         revoke_all/2,
         grant/2,
         revoke/2,
         allowed/2,
         set/2,
         set/3,
         set_resource/3,
         claim_resource/4,
         free_resource/3,
         get_resource_stat/1,
         cache/1
        ]).

-ignore_xref([ping/0]).

-define(TIMEOUT, 5000).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, snarl_user),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, snarl_user_vnode_master).

auth(User, Passwd) ->
    Hash = crypto:sha([User, Passwd]),
    {ok, Res} = snarl_entity_coverage_fsm:start(
                  {snarl_user_vnode, snarl_user},
                  auth, Hash
                 ),
    Res1 = lists:foldl(fun (not_found, Acc) ->
                               Acc;
                           (R, _) ->
                               R
                       end, not_found, Res),
    {ok, Res1}.

lookup(User) ->
    {ok, Res} = snarl_entity_coverage_fsm:start(
                  {snarl_user_vnode, snarl_user},
                  lookup, User
                 ),
    Res1 = lists:foldl(fun (not_found, Acc) ->
                               Acc;
                           (R, _) ->
                               R
                       end, not_found, Res),
    {ok, Res1}.


revoke_all(User, Perm) ->
    snarl_entity_coverage_fsm:start(
      {snarl_user_vnode, snarl_user},
      revoke_all, User, Perm).

allowed(User, Permission) ->
    case snarl_user:get(User) of
        {ok, not_found} ->
            not_found;
        {ok, UserObj} ->
            test_user(UserObj, Permission)
    end.

cache(User) ->
    case snarl_user:get(User) of
        {ok, not_found} ->
            not_found;
        {ok, UserObj} ->
            {ok, lists:foldl(
                   fun(Group, Permissions) ->
                           case snarl_group:get(Group) of
                               {ok, GroupObj} ->
                                   GrPerms = jsxd:get(<<"permissions">>, [], GroupObj),
                                   ordsets:union(Permissions, GrPerms);
                               _ ->
                                   Permissions
                           end
                   end,
                   jsxd:get(<<"permissions">>, [], UserObj),
                   jsxd:get(<<"groups">>, [], UserObj))}
    end.

get(User) ->
    snarl_entity_read_fsm:start(
      {snarl_user_vnode, snarl_user},
      get, User
     ).

list() ->
    snarl_entity_coverage_fsm:start(
      {snarl_user_vnode, snarl_user},
      list
     ).

add(User) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    case snarl_user:lookup(User) of
        {ok, not_found} ->
            ok = do_write(UUID, add, User),
            {ok, UUID};
        {ok, _UserObj} ->
            duplicate
    end.


set(User, Attribute, Value) ->
    set(User, set, [{Attribute, Value}]).

set(User, Attributes) ->
    do_write(User, set, Attributes).

passwd(User, Passwd) ->
    do_write(User, passwd, Passwd).

join(User, Group) ->
    do_write(User, join, Group).

leave(User, Group) ->
    do_write(User, leave, Group).

delete(User) ->
    do_write(User, delete).

grant(User, Permission) ->
    do_write(User, grant, Permission).

revoke(User, Permission) ->
    do_write(User, revoke, Permission).

%%%===================================================================
%%% Resource Functions
%%%===================================================================

set_resource(User, Resource, Value) ->
    do_write(User, set_resource, [Resource, Value]).

claim_resource(User, ID, Resource, Ammount) ->
    case snarl_user:get(User) of
        {ok, not_found} ->
            not_found;
        {ok, UserObj} ->
            case snarl_user_state:get_free_resource(UserObj, Resource) of
                Free when Free >= Ammount ->
                    do_write(User, claim_resource, [Resource, ID, Ammount]);
                _ ->
                    limit_reached
            end
    end.

free_resource(User, Resource, ID) ->
    do_write(User, free_resource, [Resource, ID]).

get_resource_stat(User) ->
    case snarl_user:get(User) of
        {ok, not_found} ->
            not_found;
        {ok, UserObj} ->
            snarl_user_state:get_resource_stat(UserObj)
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================


do_write(User, Op) ->
    snarl_entity_write_fsm:write({snarl_user_vnode, snarl_user}, User, Op).

do_write(User, Op, Val) ->
    snarl_entity_write_fsm:write({snarl_user_vnode, snarl_user}, User, Op, Val).

test_groups(_Permission, []) ->
    false;

test_groups(Permission, [Group|Groups]) ->
    case snarl_group:get(Group) of
        {ok, GroupObj} ->
            case libsnarlmatch:test_perms(Permission, jsxd:get(<<"permissions">>, [], GroupObj)) of
                true ->
                    true;
                false ->
                    test_groups(Permission, Groups)
            end;
        _ ->
            test_groups(Permission, Groups)
    end.

test_user(UserObj, Permission) ->
    case libsnarlmatch:test_perms(Permission, jsxd:get(<<"permissions">>, [], UserObj)) of
        true ->
            true;
        false ->
            test_groups(Permission,jsxd:get(<<"groups">>, [], UserObj))
    end.
