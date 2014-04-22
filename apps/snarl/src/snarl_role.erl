-module(snarl_role).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         sync_repair/2,
         ping/0,
         list/0, list/2,
         get/1, get_/1, raw/1,
         lookup/1, lookup_/1,
         add/1, delete/1,
         grant/2, revoke/2,
         set/2, set/3,
         create/2,
         revoke_prefix/2,
         import/2
        ]).

-ignore_xref([ping/0, create/2, raw/1, sync_repair/2]).

-define(TIMEOUT, 5000).

%% Public API

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, snarl_role),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, snarl_role_vnode_master).

import(Role, Data) ->
    do_write(Role, import, Data).

-spec lookup(Role::binary()) ->
                    not_found |
                    {error, timeout} |
                    {ok, Role::fifo:role()}.
lookup(Role) ->
    case lookup_(Role) of
        {ok, Obj} ->
            {ok, snarl_role_state:to_json(Obj)};
        R ->
            R
    end.

-spec lookup_(Role::binary()) ->
                     not_found |
                     {error, timeout} |
                     {ok, Role::#?ROLE{}}.
lookup_(Role) ->
    {ok, Res} = snarl_coverage:start(
                  snarl_role_vnode_master, snarl_role,
                  {lookup, Role}),
    R0 = lists:foldl(fun (not_found, Acc) ->
                             Acc;
                         (R, _) ->
                             {ok, R}
                     end, not_found, Res),
    case R0 of
        {ok, UUID} ->
            snarl_role:get_(UUID);
        R ->
            R
    end.

-spec get(Role::fifo:role_id()) ->
                 not_found |
                 {error, timeout} |
                 {ok, Role::fifo:role()}.
get(Role) ->
    case get_(Role) of
        {ok, RoleObj} ->
            {ok, snarl_role_state:to_json(RoleObj)};
        R  ->
            R
    end.

-spec get_(Role::fifo:role_id()) ->
                  not_found |
                  {error, timeout} |
                  {ok, Role::#?ROLE{}}.
get_(Role) ->
    case snarl_entity_read_fsm:start(
           {snarl_role_vnode, snarl_role, <<"snarl_group">>},
           get, Role
          ) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

raw(Role) ->
    snarl_entity_read_fsm:start({snarl_role_vnode, snarl_role, <<"snarl_group">>}, get,
                                Role, undefined, true).

-spec list() -> {ok, [fifo:role_id()]} |
                not_found |
                {error, timeout}.

list() ->
    snarl_coverage:start(
      snarl_role_vnode_master, snarl_role,
      list).



%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list([fifo:matcher()], boolean()) -> {error, timeout} | {ok, [fifo:uuid()]}.

list(Requirements, true) ->
    {ok, Res} = snarl_full_coverage:start(
                  snarl_role_vnode_master, snarl_role,
                  {list, Requirements, true}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)};

list(Requirements, false) ->
    {ok, Res} = snarl_coverage:start(
                  snarl_role_vnode_master, snarl_role,
                  {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

-spec add(Role::binary()) ->
                 {ok, UUID::fifo:role_id()} |
                 douplicate |
                 {error, timeout}.

add(Role) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    create(UUID, Role).

create(UUID, Role) ->
    case snarl_role:lookup_(Role) of
        not_found ->
            ok = do_write(UUID, add, Role),
            {ok, UUID};
        {ok, _RoleObj} ->
            duplicate
    end.

-spec delete(Role::fifo:role_id()) ->
                    ok |
                    not_found|
                    {error, timeout}.

delete(Role) ->
    Res = do_write(Role, delete),
    spawn(
      fun () ->
              Prefix = [<<"roles">>, Role],
              {ok, Users} = snarl_user:list(),
              [begin
                   snarl_user:leave(U, Role),
                   snarl_user:revoke_prefix(U, Prefix)
               end
               || U <- Users],
              {ok, Roles} = list(),
              [revoke_prefix(R, Prefix) || R <- Roles],
              {ok, Orgs} = snarl_org:list(),
              [snarl_org:remove_target(O, Role) || O <- Orgs]
      end),
    Res.

-spec grant(Role::fifo:role_id(), fifo:permission()) ->
                   ok |
                   not_found|
                   {error, timeout}.

grant(Role, Permission) ->
    do_write(Role, grant, Permission).

-spec revoke(Role::fifo:role_id(), fifo:permission()) ->
                    ok |
                    not_found|
                    {error, timeout}.

revoke(Role, Permission) ->
    do_write(Role, revoke, Permission).

-spec revoke_prefix(Role::fifo:role_id(), fifo:permission()) ->
                           ok |
                           not_found|
                           {error, timeout}.

revoke_prefix(Role, Prefix) ->
    do_write(Role, revoke_prefix, Prefix).

-spec set(Role::fifo:role_id(), Attirbute::fifo:key(), Value::fifo:value()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(Role, Attribute, Value) ->
    set(Role, [{Attribute, Value}]).

-spec set(Role::fifo:role_id(), Attirbutes::fifo:attr_list()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(Role, Attributes) ->
    do_write(Role, set, Attributes).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Role, Op) ->
    snarl_entity_write_fsm:write({snarl_role_vnode, snarl_role}, Role, Op).

do_write(Role, Op, Val) ->
    snarl_entity_write_fsm:write({snarl_role_vnode, snarl_role}, Role, Op, Val).
