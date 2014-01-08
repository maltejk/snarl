-module(snarl_user).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         sync_repair/2,
         ping/0,
         list/0,
         list/2,
         auth/3,
         find_key/1,
         get_/1, get/1, raw/1,
         lookup_/1, lookup/1,
         add/1, add/2,
         delete/1,
         passwd/2,
         join/2, leave/2,
         join_org/2, leave_org/2, select_org/2,
         grant/2, revoke/2, revoke_prefix/2,
         allowed/2,
         set/2,
         set/3,
         import/2,
         cache/1,
         add_key/3, revoke_key/2, keys/1,
         add_yubikey/2, remove_yubikey/2, yubikeys/1,
         active/1,
         orgs/1
        ]).

-ignore_xref([
              join_org/2, leave_org/2, select_org/2,
              lookup_/1,
              ping/0, raw/1, sync_repair/2
             ]).

-define(TIMEOUT, 5000).

%% Public API

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, snarl_user),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, snarl_user_vnode_master).

-spec find_key(KeyID::binary()) ->
                      not_found |
                      {error, timeout} |
                      {ok, User::fifo:user_id()}.
find_key(KeyID) ->
    {ok, Res} = snarl_coverage:start(
                  snarl_user_vnode_master, snarl_user,
                  {find_key, KeyID}),
    lists:foldl(fun (not_found, Acc) ->
                        Acc;
                    (R, _) ->
                        {ok, R}
                end, not_found, Res).

-ifndef(old_hash).
hash(Hash, Salt, Passwd) ->
    crypto:hash(Hash, [Salt, Passwd]).
-else.
hash(sha512, Salt, Passwd) ->
    crypto:sha512([Salt, Passwd]);
hash(sha, Salt, Pass) ->
    crypto:sha([User, Passwd]).
-endif

-spec auth(User::binary(), Passwd::binary(), OTP::binary()) ->
                  not_found |
                  {error, timeout} |
                  {ok, User::fifo:user()}.

auth(User, Passwd, OTP) ->
    case lookup_(User) of
        {ok, UserR} ->
            case snarl_user_state:password(UserR) of
                {Salt, Hash} ->
                    case hash(sha512, Salt, Passwd) of
                        Hash ->
                            case snarl_user_state:yubikeys(UserR) of
                                [] ->
                                    {ok, snarl_user_state:uuid(UserR)};
                                Ks ->
                                    YID = snarl_yubico:id(OTP),
                                    case lists:member(YID, Ks) of
                                        false ->
                                            not_found;
                                        true ->
                                            case snarl_yubico:verify(OTP) of
                                                {auth, ok} ->
                                                    {ok, snarl_user_state:uuid(UserR)};
                                                _ ->
                                                    not_found
                                            end
                                    end
                            end;
                        _ ->
                            not_found
                    end;
                Hash ->
                    case hash(sha, User, Passwd) of
                        Hash ->
                            {ok, snarl_user_state:uuid(UserR)};
                        _ ->
                            not_found
                    end
            end;
        E ->
            E
    end.

-spec lookup(User::binary()) ->
                    not_found |
                    {error, timeout} |
                    {ok, User::fifo:user()}.
lookup(User) ->
    case lookup_(User) of
        {ok, Obj} ->
            {ok, snarl_user_state:to_json(Obj)};
        R ->
            R
    end.

-spec lookup_(User::binary()) ->
                     not_found |
                     {error, timeout} |
                     {ok, User::#?USER{}}.
lookup_(User) ->
    {ok, Res} = snarl_coverage:start(
                  snarl_user_vnode_master, snarl_user,
                  {lookup, User}),
    R0 = lists:foldl(fun (not_found, Acc) ->
                             Acc;
                         (R, _) ->
                             {ok, R}
                     end, not_found, Res),
    case R0 of
        {ok, UUID} ->
            snarl_user:get_(UUID);
        R ->
            R
    end.

-spec revoke_prefix(User::fifo:user_id(),
                    Prefix::fifo:permission()) ->
                           not_found |
                           {error, timeout} |
                           ok.
revoke_prefix(User, Prefix) ->
    do_write(User, revoke_prefix, Prefix).

-spec allowed(User::fifo:uuid(),
              Permission::fifo:permission()) ->
                     not_found |
                     {error, timeout} |
                     true | false.
allowed(User, Permission) ->
    case get_(User) of
        {ok, UserObj} ->
            test_user(UserObj, Permission);
        E ->
            E
    end.

add_key(User, KeyID, Key) ->
    do_write(User, add_key, {KeyID, Key}).

revoke_key(User, KeyID) ->
    do_write(User, revoke_key, KeyID).

keys(User) ->
    case get_(User) of
        {ok, UserObj} ->
            {ok, snarl_user_state:keys(UserObj)};
        E ->
            E
    end.

add_yubikey(User, OTP) ->
    KeyID = snarl_yubico:id(OTP),
    do_write(User, add_yubikey, KeyID).

remove_yubikey(User, KeyID) ->
    do_write(User, remove_yubikey, KeyID).

yubikeys(User) ->
    case get_(User) of
        {ok, UserObj} ->
            {ok, snarl_user_state:yubikeys(UserObj)};
        E ->
            E
    end.

active(User) ->
    case get_(User) of
        {ok, UserObj} ->
            {ok, snarl_user_state:active_org(UserObj)};
        E ->
            E
    end.

orgs(User) ->
    case get_(User) of
        {ok, UserObj} ->
            {ok, snarl_user_state:orgs(UserObj)};
        E ->
            E
    end.

-spec cache(User::fifo:user_id()) ->
                   not_found |
                   {error, timeout} |
                   {ok, Perms::[fifo:permission()]}.
cache(User) ->
    case get_(User) of
        {ok, UserObj} ->
            {ok, lists:foldl(
                   fun(Group, Permissions) ->
                           case snarl_group:get_(Group) of
                               {ok, GroupObj} ->
                                   GrPerms = snarl_group_state:permissions(GroupObj),
                                   ordsets:union(Permissions, GrPerms);
                               _ ->
                                   Permissions
                           end
                   end,
                   snarl_user_state:permissions(UserObj),
                   snarl_user_state:groups(UserObj))};
        E ->
            E
    end.


-spec get(User::fifo:user_id()) ->
                 not_found |
                 {error, timeout} |
                 {ok, User::fifo:user()}.
get(User) ->
    case get_(User) of
        {ok, UserObj} ->
            {ok, snarl_user_state:to_json(UserObj)};
        R  ->
            R
    end.

-spec get_(User::fifo:user_id()) ->
                  not_found |
                  {error, timeout} |
                  {ok, User::#?USER{}}.
get_(User) ->
    case snarl_entity_read_fsm:start(
           {snarl_user_vnode, snarl_user},
           get, User
          ) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

raw(User) ->
    snarl_entity_read_fsm:start({snarl_user_vnode, snarl_user}, get,
                                User, undefined, true).

-spec list() ->
                  {error, timeout} |
                  {ok, Users::[fifo:user_id()]}.
list() ->
    snarl_coverage:start(
      snarl_user_vnode_master, snarl_user,
      list).

-spec list(Reqs::[fifo:matcher()]) ->
                  {ok, [IPR::fifo:user_id()]} | {error, timeout}.
list(Requirements) ->
    {ok, Res} = snarl_coverage:start(
                  snarl_user_vnode_master, snarl_user,
                  {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.


-spec list([fifo:matcher()], boolean()) -> {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements, true) ->
    {ok, Ls} = list(Requirements),
    Ls1 = [{V, {UUID, ?MODULE:get(UUID)}} || {V, UUID} <- Ls],
    Ls2 = [{V, {UUID, D}} || {V, {UUID, {ok, D}}} <- Ls1],
    {ok,  Ls2};
list(Requirements, false) ->
    list(Requirements).

-spec add(Creator::fifo:user_id(),
          UserName::binary()) ->
                 duplicate |
                 {error, timeout} |
                 {ok, UUID::fifo:user_id()}.

add(Creator, User) when is_binary(Creator),
                        is_binary(User) ->
    case add(undefined, User) of
        {ok, UUID} = R ->
            case get_(Creator) of
                {ok, C} ->
                    case snarl_user_state:active_org(C) of
                        <<>> ->
                            R;
                        Org ->
                            snarl_org:trigger(Org, user_create, UUID),
                            R
                    end;
                _ ->
                    R
            end;
        E ->
            E
    end;

add(_, User) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    create(UUID, User).

add(User) ->
    add(undefined, User).

create(UUID, User) ->
    case lookup_(User) of
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
-ifndef(old_hash).
passwd(User, Passwd) ->
    Salt = crypto:rand_bytes(64),
    Hash = crypto:hash(sha512, [Salt, Passwd]),
    do_write(User, passwd, {Salt, Hash}).
-else.
passwd(User, Passwd) ->
    Salt = crypto:rand_bytes(64),
    Hash = crypto:sha512([Salt, Passwd]),
    do_write(User, passwd, {Salt, Hash}).
-endif.

import(User, Data) ->
    do_write(User, import, Data).

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


-spec join_org(User::fifo:user_id(), Org::fifo:org_id()) ->
                      not_found |
                      {error, timeout} |
                      ok.
join_org(User, Org) ->
    do_write(User, join_org, Org).

-spec select_org(User::fifo:user_id(), Org::fifo:org_id()) ->
                        not_found |
                        {error, timeout} |
                        ok.
select_org(User, Org) ->
    case get_(User) of
        {ok, UserObj} ->
            Orgs = snarl_user_state:orgs(UserObj),
            case lists:member(Org, Orgs) of
                true ->
                    do_write(User, select_org, Org);
                _ ->
                    not_found
            end;
        R  ->
            R
    end.

-spec leave_org(User::fifo:user_id(), Org::fifo:org_id()) ->
                       not_found |
                       {error, timeout} |
                       ok.
leave_org(User, Org) ->
    case get_(User) of
        {ok, UserObj} ->
            case snarl_user_state:active_org(UserObj) of
                Org ->
                    do_write(User, select_org, <<"">>);
                _ ->
                    ok
            end,
            do_write(User, leave_org, Org);
        R  ->
            R
    end.

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
    case snarl_group:get_(Group) of
        {ok, GroupObj} ->
            case libsnarlmatch:test_perms(
                   Permission,
                   snarl_group_state:permissions(GroupObj)) of
                true ->
                    true;
                false ->
                    test_groups(Permission, Groups)
            end;
        _ ->
            test_groups(Permission, Groups)
    end.

test_user(UserObj, Permission) ->
    case libsnarlmatch:test_perms(
           Permission,
           snarl_user_state:permissions(UserObj)) of
        true ->
            true;
        false ->
            test_groups(Permission, snarl_user_state:groups(UserObj))
    end.
