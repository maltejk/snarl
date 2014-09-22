-module(snarl_user).
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         sync_repair/3,
         list/0,
         list/1,
         list_/1,
         list/3,
         auth/4,
         reindex/2,
         find_key/2,
         get/2, raw/2,
         lookup_/2, lookup/2,
         add/2, add/3,
         delete/2,
         passwd/3,
         join/3, leave/3,
         join_org/3, leave_org/3, select_org/3,
         grant/3, revoke/3, revoke_prefix/3,
         allowed/3,
         set_metadata/3,
         import/3,
         cache/2,
         add_key/4, revoke_key/3, keys/2,
         add_yubikey/3, remove_yubikey/3, yubikeys/2,
         active/2,
         orgs/2,
         wipe/2
        ]).

-ignore_xref([
              import/3,
              wipe/2,
              join_org/3, leave_org/3, select_org/3,
              lookup_/2, list_/1,
              raw/2, sync_repair/3,
              reindex/2
             ]).

-define(TIMEOUT, 5000).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {snarl, user, Met},
          Mod, Fun, Args)).

-define(NAME_2i, {snarl_user, name}).
-define(KEY_2i, {snarl_user, key}).

reindex(Realm, UUID) ->
    case ?MODULE:get(Realm, UUID) of
        {ok, O} ->
            snarl_2i:add(Realm, ?NAME_2i, ft_user:name(O), UUID),
            [snarl_2i:add(Realm, ?KEY_2i, key_to_id(K), UUID) ||
                {_, K} <- ft_user:keys(O)],
            ok;
        E ->
            E
    end.

%% Public API
wipe(Realm, UUID) ->
    ?FM(wipe, snarl_coverage, start,
        [snarl_user_vnode_master, snarl_user, {wipe, Realm, UUID}]).

sync_repair(Realm, UUID, Obj) ->
    do_write(Realm, UUID, sync_repair, Obj).

-spec find_key(Realm::binary(), KeyID::binary()) ->
                      not_found |
                      {error, timeout} |
                      {ok, User::fifo:user_id()}.
find_key(Realm, KeyID) ->
    folsom_metrics:histogram_timed_update(
      {snarl, user, find_key},
      snarl_2i, get, [Realm, ?KEY_2i, KeyID]).

-spec auth(Realm::binary(), User::binary(), Passwd::binary(),
           OTP::binary()|basic) ->
                  not_found |
                  {error, timeout} |
                  {ok, User::fifo:user_id()}.

auth(Realm, User, Passwd, basic) ->
    case snarl_user:get(Realm, User) of
        {ok, UserR} ->
            case check_pw(UserR, Passwd) of
                true ->
                    {ok, ft_user:uuid(UserR)};
                _ ->
                    not_found
            end;
        E ->
            E
    end;

auth(Realm, User, Passwd, OTP) ->
    Res1 = case lookup_(Realm, User) of
               {ok, UserR} ->
                   case check_pw(UserR, Passwd) of
                       true ->
                           {ok, UserR};
                       _ ->
                           not_found
                   end;
               E ->
                   E
           end,
    case Res1 of
        {ok, UserR1} ->
            case ft_user:yubikeys(UserR1) of
                [] ->
                    {ok, ft_user:uuid(UserR1)};
                Ks ->
                    case snarl_yubico:id(OTP) of
                        <<>> ->
                            key_required;
                        YID  ->
                            case lists:member(YID, Ks) of
                                false ->
                                    not_found;
                                true ->
                                    case snarl_yubico:verify(OTP) of
                                        {auth, ok} ->
                                            {ok, ft_user:uuid(UserR1)};
                                        _ ->
                                            not_found
                                    end
                            end
                    end
            end;
        E1 ->
            E1
    end.

-spec lookup(Realm::binary(), User::binary()) ->
                    not_found |
                    {error, timeout} |
                    {ok, User::fifo:user()}.
lookup(Realm, User) ->
    case lookup_(Realm, User) of
        {ok, Obj} ->
            {ok, ft_user:to_json(Obj)};
        R ->
            R
    end.

-spec lookup_(Realm::binary(), User::binary()) ->
                     not_found |
                     {error, timeout} |
                     {ok, User::fifo:user()}.
lookup_(Realm, User) ->
    folsom_metrics:histogram_timed_update(
      {snarl, user, lookup},
      fun() ->
              case snarl_2i:get(Realm, ?NAME_2i, User) of
                  {ok, UUID} ->
                      snarl_user:get(Realm, UUID);
                  R ->
                      R
              end
      end).

-spec revoke_prefix(Realm::binary(),
                    User::fifo:user_id(),
                    Prefix::fifo:permission()) ->
                           not_found |
                           {error, timeout} |
                           ok.
revoke_prefix(Realm, User, Prefix) ->
    do_write(Realm, User, revoke_prefix, Prefix).

-spec allowed(Realm::binary(),
              User::fifo:uuid(),
              Permission::fifo:permission()) ->
                     not_found |
                     {error, timeout} |
                     true | false.
allowed(Realm, User, Permission) ->
    case snarl_user:get(Realm, User) of
        {ok, UserObj} ->
            test_user(Realm, UserObj, Permission);
        E ->
            E
    end.

add_key(Realm, User, KeyID, Key) ->
    SSHID = key_to_id(Key),
    case snarl_2i:get(Realm, ?KEY_2i, SSHID) of
        {ok, _} ->
            {error, duplicate};
        _ ->
            case do_write(Realm, User, add_key, {KeyID, Key}) of
                ok ->
                    snarl_2i:add(Realm, ?KEY_2i, SSHID, User),
                    ok;
                E ->
                    E
            end
    end.

revoke_key(Realm, User, KeyID) ->
    case ?MODULE:get(Realm, User) of
        {ok, U} ->
            Ks = ft_user:keys(U),
            case jsxd:get(KeyID, Ks) of
                {ok, Key} ->
                    snarl_2i:delete(Realm, ?KEY_2i, key_to_id(Key)),
                    do_write(Realm, User, revoke_key, KeyID);
                _ ->
                    ok
            end;
        E ->
            E
    end.

keys(Realm, User) ->
    case snarl_user:get(Realm, User) of
        {ok, UserObj} ->
            {ok, ft_user:keys(UserObj)};
        E ->
            E
    end.

add_yubikey(Realm, User, OTP) ->
    KeyID = snarl_yubico:id(OTP),
    do_write(Realm, User, add_yubikey, KeyID).

remove_yubikey(Realm, User, KeyID) ->
    do_write(Realm, User, remove_yubikey, KeyID).

yubikeys(Realm, User) ->
    case snarl_user:get(Realm, User) of
        {ok, UserObj} ->
            {ok, ft_user:yubikeys(UserObj)};
        E ->
            E
    end.

active(Realm, User) ->
    case snarl_user:get(Realm, User) of
        {ok, UserObj} ->
            {ok, ft_user:active_org(UserObj)};
        E ->
            E
    end.

orgs(Realm, User) ->
    case snarl_user:get(Realm, User) of
        {ok, UserObj} ->
            {ok, ft_user:orgs(UserObj)};
        E ->
            E
    end.

-spec cache(Realm::binary(), User::fifo:user_id()) ->
                   not_found |
                   {error, timeout} |
                   {ok, Perms::[fifo:permission()]}.
cache(Realm, User) ->
    case snarl_user:get(Realm, User) of
        {ok, UserObj} ->
            {ok, lists:foldl(
                   fun(Role, Permissions) ->
                           case snarl_role:get(Realm, Role) of
                               {ok, RoleObj} ->
                                   GrPerms = ft_role:permissions(RoleObj),
                                   ordsets:union(Permissions, GrPerms);
                               _ ->
                                   Permissions
                           end
                   end,
                   ft_user:permissions(UserObj),
                   ft_user:roles(UserObj))};
        E ->
            E
    end.

-spec get(Realm::binary(), User::fifo:user_id()) ->
                  not_found |
                  {error, timeout} |
                  {ok, User::fifo:user()}.
get(Realm, User) ->
    case ?FM(get, snarl_entity_read_fsm, start,
             [{snarl_user_vnode, snarl_user}, get, {Realm, User}]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

raw(Realm, User) ->
    case ?FM(get, snarl_entity_read_fsm, start,
             [{snarl_user_vnode, snarl_user}, get, {Realm, User}, undefined,
              true]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

-spec list(Realm::binary()) ->
                  {error, timeout} |
                  {ok, Users::[fifo:user_id()]}.
list(Realm) ->
    ?FM(list, snarl_coverage, start,
        [snarl_user_vnode_master, snarl_user, {list, Realm}]).

list() ->
    ?FM(list_all, snarl_coverage, start,
        [snarl_user_vnode_master, snarl_user, list]).

list_(Realm) ->
    {ok, Res} =
        ?FM(list, snarl_full_coverage, start,
            [snarl_user_vnode_master, snarl_user,
             {list, Realm, [], true, true}]),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec list(Realm::binary(), [fifo:matcher()], boolean()) ->
                  {error, timeout} | {ok, [fifo:uuid()]}.

list(Realm, Requirements, Full)
  when Full == true orelse Full == false ->
    {ok, Res} =
        ?FM(list, snarl_full_coverage, start,
            [snarl_user_vnode_master, snarl_user,
             {list, Realm, Requirements, Full}]),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

-spec add(Realm::binary(), Creator::fifo:user_id(),
          UserName::binary()) ->
                 duplicate |
                 {error, timeout} |
                 {ok, UUID::fifo:user_id()}.

add(Realm, undefined, User) ->
    UUID = uuid:uuid4s(),
    lager:info("[~p:create] Creation Started.", [UUID]),
    case create(Realm, UUID, User) of
        {ok, UUID} ->
            lager:info("[~p:create] Created.", [UUID]),
            case snarl_opt:get(users, Realm,
                               inital_role,
                               user_inital_role, undefined) of
                undefined ->
                    lager:info("[~p:create] No default role.",
                               [UUID]),
                    ok;
                Grp ->
                    lager:info("[~p:create] Assigning default role: ~s.",
                               [UUID, Grp]),
                    join(Realm, UUID, Grp)
            end,
            {ok, UUID};
        E ->
            lager:error("[create] Failed to create: ~p.", [E]),
            E
    end;

add(Realm, Creator, User) when is_binary(Creator),
                               is_binary(User) ->
    case add(Realm, undefined, User) of
        {ok, UUID} = R ->
            case snarl_user:get(Realm, Creator) of
                {ok, C} ->
                    case ft_user:active_org(C) of
                        <<>> ->
                            lager:info("[~s:create] Creator ~s has no "
                                       "active organisation.",
                                       [UUID, Creator]),
                            R;
                        Org ->
                            lager:info("[~s:create] Triggering org user "
                                       "creation for organisation ~s",
                                       [UUID, Org]),
                            snarl_org:trigger(Realm, Org, user_create, UUID),
                            R
                    end;
                E ->
                    lager:warning("[~s:create] Failed to get creator ~s: ~p.",
                                  [UUID, Creator, E]),
                    R
            end;
        E ->
            E
    end.

add(Realm, User) ->
    add(Realm, undefined, User).

create(Realm, UUID, User) ->
    case lookup_(Realm, User) of
        not_found ->
            ok = do_write(Realm, UUID, add, User),
            snarl_2i:add(Realm, ?NAME_2i, User, UUID),
            {ok, UUID};
        {ok, _UserObj} ->
            duplicate
    end.

-spec set_metadata(Realm::binary(), User::fifo:user_id(),
                   Attirbutes::fifo:attr_list()) ->
                          not_found |
                          {error, timeout} |
                          ok.
set_metadata(Realm, User, Attributes) ->
    do_write(Realm, User, set_metadata, Attributes).

-spec passwd(Realm::binary(), User::fifo:user_id(), Passwd::binary()) ->
                    not_found |
                    {error, timeout} |
                    ok.
passwd(Realm, User, Passwd) ->
    H = case application:get_env(snarl, hash_fun) of
            {ok, sha512} ->
                Salt = crypto:rand_bytes(64),
                Hash = hash(sha512, Salt, Passwd),
                {Salt, Hash};
            _ ->
                {ok, Salt} = bcrypt:gen_salt(),
                {ok, Hash} = bcrypt:hashpw(Passwd, Salt),
                {bcrypt, list_to_binary(Hash)}
        end,
    do_write(Realm, User, passwd, H).

import(Realm, User, Data) ->
    do_write(Realm, User, import, Data).

-spec join(Realm::binary(), User::fifo:user_id(), Role::fifo:role_id()) ->
                  not_found |
                  {error, timeout} |
                  ok.
join(Realm, User, Role) ->
    case snarl_role:get(Realm, Role) of
        {ok, _} ->
            do_write(Realm, User, join, Role);
        E ->
            E
    end.

-spec leave(Realm::binary(), User::fifo:user_id(), Role::fifo:role_id()) ->
                   not_found |
                   {error, timeout} |
                   ok.
leave(Realm, User, Role) ->
    do_write(Realm, User, leave, Role).


-spec join_org(Realm::binary(), User::fifo:user_id(), Org::fifo:org_id()) ->
                      not_found |
                      {error, timeout} |
                      ok.
join_org(Realm, User, Org) ->
    case snarl_org:get(Realm, Org) of
        {ok, _} ->
            do_write(Realm, User, join_org, Org);
        E ->
            E
    end.

-spec select_org(Realm::binary(), User::fifo:user_id(), Org::fifo:org_id()) ->
                        not_found |
                        {error, timeout} |
                        ok.
select_org(Realm, User, Org) ->
    case snarl_user:get(Realm, User) of
        {ok, UserObj} ->
            Orgs = ft_user:orgs(UserObj),
            case lists:member(Org, Orgs) of
                true ->
                    do_write(Realm, User, select_org, Org);
                _ ->
                    not_found
            end;
        R  ->
            R
    end.

-spec leave_org(Realm::binary(), User::fifo:user_id(), Org::fifo:org_id()) ->
                       not_found |
                       {error, timeout} |
                       ok.
leave_org(Realm, User, Org) ->
    case snarl_user:get(Realm, User) of
        {ok, UserObj} ->
            case ft_user:active_org(UserObj) of
                Org ->
                    do_write(Realm, User, select_org, <<"">>);
                _ ->
                    ok
            end,
            do_write(Realm, User, leave_org, Org);
        R  ->
            R
    end.

-spec delete(Realm::binary(), User::fifo:user_id()) ->
                    not_found |
                    {error, timeout} |
                    ok.
delete(Realm, User) ->
    case ?MODULE:get(Realm, User) of
        {ok, O} ->
            snarl_2i:delete(Realm, ?NAME_2i, ft_user:name(O)),
            [snarl_2i:delete(Realm, ?KEY_2i, key_to_id(K)) ||
                {_, K} <- ft_user:keys(O)],
            ok;
        E ->
            E
    end,
    spawn(
      fun () ->
              Prefix = [<<"users">>, User],
              {ok, Users} = snarl_user:list(Realm),
              [snarl_user:revoke_prefix(Realm, U, Prefix) || U <- Users],
              {ok, Roles} = snarl_role:list(Realm),
              [snarl_role:revoke_prefix(Realm, R, Prefix) || R <- Roles],
              {ok, Orgs} = snarl_org:list(Realm),
              [snarl_org:remove_target(Realm, O, User) || O <- Orgs]
      end),
    do_write(Realm, User, delete).

-spec grant(Realm::binary(), User::fifo:user_id(),
            Permission::fifo:permission()) ->
                   not_found |
                   {error, timeout} |
                   ok.
grant(Realm, User, Permission) ->
    do_write(Realm, User, grant, Permission).

-spec revoke(Realm::binary(), User::fifo:user_id(),
             Permission::fifo:permission()) ->
                    not_found |
                    {error, timeout} |
                    ok.
revoke(Realm, User, Permission) ->
    do_write(Realm, User, revoke, Permission).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Realm, User, Op) ->
    case ?FM(Op, snarl_entity_write_fsm, write,
             [{snarl_user_vnode, snarl_user}, {Realm, User}, Op]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

do_write(Realm, User, Op, Val) ->
    case ?FM(Op, snarl_entity_write_fsm, write,
             [{snarl_user_vnode, snarl_user}, {Realm, User}, Op, Val]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

test_roles(_Realm, _Permission, []) ->
    false;

test_roles(Realm, Permission, [Role|Roles]) ->
    case snarl_role:get(Realm, Role) of
        {ok, RoleObj} ->
            case libsnarlmatch:test_perms(
                   Permission,
                   ft_role:permissions(RoleObj)) of
                true ->
                    true;
                false ->
                    test_roles(Realm, Permission, Roles)
            end;
        _ ->
            test_roles(Realm, Permission, Roles)
    end.

test_user(Realm, UserObj, Permission) ->
    case libsnarlmatch:test_perms(
           Permission,
           ft_user:permissions(UserObj)) of
        true ->
            true;
        false ->
            test_roles(Realm, Permission, ft_user:roles(UserObj))
    end.

check_pw(UserR, Passwd) ->
    case ft_user:password(UserR) of
        {bcrypt, Hash} ->
            HashS = binary_to_list(Hash),
            case bcrypt:hashpw(Passwd, Hash) of
                {ok, HashS} ->
                    true;
                _ ->
                    false
            end;
        {S, H} ->
            case hash(sha512, S, Passwd) of
                H ->
                    true;
                _ ->
                    false
            end;
        %% Unset passwords are always false
        <<>> ->
            false
    end.

-ifndef(old_hash).
hash(Hash, Salt, Passwd) ->
    crypto:hash(Hash, [Salt, Passwd]).
-else.
hash(sha512, Salt, Passwd) ->
    crypto:sha512([Salt, Passwd]).
-endif.


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
