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
         revoke_prefix/2,
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


-spec auth(User::binary(), Passwd::binary()) ->
                  not_found |
                  {error, timeout} |
                  {ok, User::fifo:user()}.
auth(User, Passwd) ->
    Hash = crypto:sha([User, Passwd]),
    {ok, Res} = snarl_entity_coverage_fsm:start(
                  {snarl_user_vnode, snarl_user},
                  auth, Hash
                 ),
    lists:foldl(fun (not_found, Acc) ->
                        Acc;
                    (R, _) ->
                        {ok, R}
                end, not_found, Res).

-spec lookup(User::binary()) ->
                    not_found |
                    {error, timeout} |
                    {ok, User::fifo:user()}.
lookup(User) ->
    {ok, Res} = snarl_entity_coverage_fsm:start(
                  {snarl_user_vnode, snarl_user},
                  lookup, User),
    lists:foldl(fun (not_found, Acc) ->
                        Acc;
                    (R, _) ->
                        {ok, R}
                end, not_found, Res).

-spec revoke_prefix(User::fifo:user_id(),
                 Perm::fifo:permission()) ->
                        not_found |
                        {error, timeout} |
                        ok.
revoke_prefix(User, Perm) ->
    snarl_entity_coverage_fsm:start(
      {snarl_user_vnode, snarl_user},
      revoke_prefix, User, Perm).

-spec allowed(User::fifo:uuid(),
              Permission::fifo:permission()) ->
                     not_found |
                     {error, timeout} |
                     true | false.
allowed(User, Permission) ->
    case snarl_user:get(User) of
        {ok, UserObj} ->
            test_user(UserObj, Permission);
        E ->
            E
    end.

-spec cache(User::fifo:user_id()) ->
                   not_found |
                   {error, timeout} |
                   {ok, Perms::[fifo:permission()]}.
cache(User) ->
    case snarl_user:get(User) of
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
                   jsxd:get(<<"groups">>, [], UserObj))};
        E ->
            E
    end.

-spec get(User::fifo:user_id()) ->
                 not_found |
                 {error, timeout} |
                 {ok, User::fifo:user()}.
get(User) ->
    case snarl_entity_read_fsm:start(
           {snarl_user_vnode, snarl_user},
           get, User
          ) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

-spec list() ->
                  {error, timeout} |
                  {ok, Users::[fifo:user_id()]}.
list() ->
    snarl_entity_coverage_fsm:start(
      {snarl_user_vnode, snarl_user},
      list
     ).

-spec add(UserName::binary()) ->
                 duplicate |
                 {error, timeout} |
                 {ok, UUID::fifo:user_id()}.
add(User) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    case snarl_user:lookup(User) of
        not_found ->
            ok = do_write(UUID, add, User),
            {ok, UUID};
        {ok, _UserObj} ->
            duplicate
    end.

-spec set(User::fifo:user_id(), Attirbute::fifo:key(), Value::fifo:value()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(User, Attribute, Value) ->
    set(User, [{Attribute, Value}]).

-spec set(User::fifo:user_id(), Attirbutes::fifo:attr_list()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(User, Attributes) ->
    do_write(User, set, Attributes).

-spec passwd(User::fifo:user_id(), Passwd::binary()) ->
                    not_found |
                    {error, timeout} |
                    ok.
passwd(User, Passwd) ->
    do_write(User, passwd, Passwd).

-spec join(User::fifo:user_id(), Group::fifo:group_id()) ->
                  not_found |
                  {error, timeout} |
                  ok.
join(User, Group) ->
    do_write(User, join, Group).

-spec leave(User::fifo:user_id(), Group::fifo:group_id()) ->
                   not_found |
                   {error, timeout} |
                   ok.
leave(User, Group) ->
    do_write(User, leave, Group).

-spec delete(User::fifo:user_id()) ->
                    not_found |
                    {error, timeout} |
                    ok.
delete(User) ->
    do_write(User, delete).

-spec grant(User::fifo:user_id(),
            Permission::fifo:permission()) ->
                   not_found |
                   {error, timeout} |
                   ok.
grant(User, Permission) ->
    do_write(User, grant, Permission).

-spec revoke(User::fifo:user_id(),
             Permission::fifo:permission()) ->
                    not_found |
                    {error, timeout} |
                    ok.
revoke(User, Permission) ->
    do_write(User, revoke, Permission).

%%%===================================================================
%%% Resource Functions
%%%===================================================================

set_resource(User, Resource, Value) ->
    do_write(User, set_resource, [Resource, Value]).

claim_resource(User, ID, Resource, Ammount) ->
    case snarl_user:get(User) of
        {ok, UserObj} ->
            case snarl_user_state:get_free_resource(Resource, UserObj) of
                Free when Free >= Ammount ->
                    do_write(User, claim_resource, [Resource, ID, Ammount]);
                _ ->
                    limit_reached
            end;
        E ->
            E
    end.

free_resource(User, Resource, ID) ->
    do_write(User, free_resource, [Resource, ID]).

get_resource_stat(User) ->
    case snarl_user:get(User) of
        {ok, UserObj} ->
            snarl_user_state:get_resource_stat(UserObj);
        E ->
            E
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================


do_write(User, Op) ->
    case snarl_entity_write_fsm:write({snarl_user_vnode, snarl_user}, User, Op) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

do_write(User, Op, Val) ->
    case snarl_entity_write_fsm:write({snarl_user_vnode, snarl_user}, User, Op, Val) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

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
