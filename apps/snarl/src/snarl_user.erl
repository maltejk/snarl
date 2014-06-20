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
         list_/0,
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
         orgs/1,
         wipe/1
        ]).

-ignore_xref([
              wipe/1,
              list_/0,
              join_org/2, leave_org/2, select_org/2,
              lookup_/1, list_/0,
              ping/0, raw/1, sync_repair/2
             ]).

-define(TIMEOUT, 5000).

%% Public API
wipe(UUID) ->
    snarl_coverage:start(snarl_user_vnode_master, snarl_user,
                         {wipe, UUID}).

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

-spec auth(User::binary(), Passwd::binary(), OTP::binary()|basic) ->
                  not_found |
                  {error, timeout} |
                  {ok, User::fifo:user()}.

auth(User, Passwd, basic) ->
    case get_(User) of
        {ok, UserR} ->
            case check_pw(UserR, Passwd) of
                true ->
                    {ok, snarl_user_state:uuid(UserR)};
                _ ->
                    not_found
            end;
        E ->
            E
    end;

auth(User, Passwd, OTP) ->
    Res1 = case lookup_(User) of
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
            case snarl_user_state:yubikeys(UserR1) of
                [] ->
                    {ok, snarl_user_state:uuid(UserR1)};
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
                                            {ok, snarl_user_state:uuid(UserR1)};
                                        _ ->
                                            not_found
                                    end
                            end
                    end
            end;
        E1 ->
            E1
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
                   fun(Role, Permissions) ->
                           case snarl_role:get_(Role) of
                               {ok, RoleObj} ->
                                   GrPerms = snarl_role_state:permissions(RoleObj),
                                   ordsets:union(Permissions, GrPerms);
                               _ ->
                                   Permissions
                           end
                   end,
                   snarl_user_state:permissions(UserObj),
                   snarl_user_state:roles(UserObj))};
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
           get, User) of
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

list_() ->
    {ok, Res} = snarl_full_coverage:start(
                  snarl_user_vnode_master, snarl_user,
                  {list, [], true, true}),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec list([fifo:matcher()], boolean()) -> {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements, true) ->
    {ok, Res} = snarl_full_coverage:start(
                  snarl_user_vnode_master, snarl_user,
                  {list, Requirements, true}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)};

list(Requirements, false) ->
    {ok, Res} = snarl_coverage:start(
                  snarl_user_vnode_master, snarl_user,
                  {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

-spec add(Creator::fifo:user_id(),
          UserName::binary()) ->
                 duplicate |
                 {error, timeout} |
                 {ok, UUID::fifo:user_id()}.

add(undefined, User) ->
    UUID = uuid:uuid4s(),
    lager:info("[~p:create] Creation Started.", [UUID]),
    case create(UUID, User) of
        {ok, UUID} ->
            lager:info("[~p:create] Created.", [UUID]),
            case snarl_opt:get(defaults, users,
                               inital_role,
                               user_inital_role, undefined) of
                undefined ->
                    lager:info("[~p:create] No default role.",
                               [UUID]),
                    ok;
                Grp ->
                    lager:info("[~p:create] Assigning default role: ~s.",
                               [UUID, Grp]),
                    join(UUID, Grp)
            end,
            {ok, UUID};
        E ->
            lager:error("[create] Failed to create: ~p.", [E]),
            E
    end;

add(Creator, User) when is_binary(Creator),
                        is_binary(User) ->
    case add(undefined, User) of
        {ok, UUID} = R ->
            case get_(Creator) of
                {ok, C} ->
                    case snarl_user_state:active_org(C) of
                        <<>> ->
                            lager:info("[~s:create] Creator ~s has no "
                                       "active organisation.",
                                       [UUID, Creator]),
                            R;
                        Org ->
                            lager:info("[~s:create] Triggering org user "
                                       "creation for organisation ~s",
                                       [UUID, Org]),
                            snarl_org:trigger(Org, user_create, UUID),
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
passwd(User, Passwd) ->
    H = case application:get_env(snarl, hash_fun) of
            {ok, sha512} ->
                Salt = crypto:rand_bytes(64),
                Hash = hash(sha512, Salt, Passwd),

                {Salt, Hash};
            %% {ok, bcrypt} ->
            %% undefined ->
            _ ->
                {ok, Salt} = bcrypt:gen_salt(),
                {ok, Hash} = bcrypt:hashpw(Passwd, Salt),
                {bcrypt, list_to_binary(Hash)}
        end,
    do_write(User, passwd, H).

import(User, Data) ->
    do_write(User, import, Data).

-spec join(User::fifo:user_id(), Role::fifo:role_id()) ->
                  not_found |
                  {error, timeout} |
                  ok.
join(User, Role) ->
    case snarl_role:get_(Role) of
        {ok, _} ->
            do_write(User, join, Role);
        E ->
            E
    end.

-spec leave(User::fifo:user_id(), Role::fifo:role_id()) ->
                   not_found |
                   {error, timeout} |
                   ok.
leave(User, Role) ->
    do_write(User, leave, Role).


-spec join_org(User::fifo:user_id(), Org::fifo:org_id()) ->
                      not_found |
                      {error, timeout} |
                      ok.
join_org(User, Org) ->
    case snarl_org:get_(Org) of
        {ok, _} ->
            do_write(User, join_org, Org);
        E ->
            E
    end.

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
    Res = do_write(User, delete),
    spawn(
      fun () ->
              Prefix = [<<"users">>, User],
              {ok, Users} = snarl_user:list(),
              [snarl_user:revoke_prefix(U, Prefix) || U <- Users],
              {ok, Roles} = list(),
              [snarl_role:revoke_prefix(R, Prefix) || R <- Roles],
              {ok, Orgs} = snarl_org:list(),
              [snarl_org:remove_target(O, User) || O <- Orgs]
      end),
    Res.

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

test_roles(_Permission, []) ->
    false;

test_roles(Permission, [Role|Roles]) ->
    case snarl_role:get_(Role) of
        {ok, RoleObj} ->
            case libsnarlmatch:test_perms(
                   Permission,
                   snarl_role_state:permissions(RoleObj)) of
                true ->
                    true;
                false ->
                    test_roles(Permission, Roles)
            end;
        _ ->
            test_roles(Permission, Roles)
    end.

test_user(UserObj, Permission) ->
    case libsnarlmatch:test_perms(
           Permission,
           snarl_user_state:permissions(UserObj)) of
        true ->
            true;
        false ->
            test_roles(Permission, snarl_user_state:roles(UserObj))
    end.

check_pw(UserR, Passwd) ->
    case snarl_user_state:password(UserR) of
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
            end
    end.

-ifndef(old_hash).
hash(Hash, Salt, Passwd) ->
    crypto:hash(Hash, [Salt, Passwd]).
-else.
hash(sha512, Salt, Passwd) ->
    crypto:sha512([Salt, Passwd]).
-endif.
