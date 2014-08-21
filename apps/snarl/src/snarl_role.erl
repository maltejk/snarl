-module(snarl_role).

-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("fifo_dt/include/ft.hrl").

-export([
         sync_repair/3,
         list/0, list/1, list/3, list_/1,
         get/2, get_/2, raw/2,
         lookup/2, lookup_/2,
         add/2, delete/2,
         grant/3, revoke/3,
         set/3, set/4,
         create/3,
         revoke_prefix/3,
         import/3, wipe/2
        ]).

-ignore_xref([
              wipe/2,
              list_/1,
              create/3, raw/2, sync_repair/3,
              import/3, lookup/2
             ]).

-define(TIMEOUT, 5000).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {snarl, role, Met},
          Mod, Fun, Args)).

%% Public API

wipe(Realm, UUID) ->
    ?FM(wipe, snarl_coverage, start,
        [snarl_role_vnode_master, snarl_role, {wipe, Realm, UUID}]).

sync_repair(Realm, UUID, Obj) ->
    do_write(Realm, UUID, sync_repair, Obj).

import(Realm, Role, Data) ->
    do_write(Realm, Role, import, Data).

-spec lookup(Realm::binary(), Role::binary()) ->
                    not_found |
                    {error, timeout} |
                    {ok, Role::fifo:role()}.
lookup(Realm, Role) ->
    case lookup_(Realm, Role) of
        {ok, Obj} ->
            {ok, ft_role:to_json(Obj)};
        R ->
            R
    end.

-spec lookup_(Realm::binary(), Role::binary()) ->
                     not_found |
                     {error, timeout} |
                     {ok, Role::#?ROLE{}}.
lookup_(Realm, Role) ->
    {ok, Res} =
        ?FM(lookup, snarl_coverage,start,
            [snarl_role_vnode_master, snarl_role, {lookup, Realm, Role}]),
    R0 = lists:foldl(fun (not_found, Acc) ->
                             Acc;
                         (R, _) ->
                             {ok, R}
                     end, not_found, Res),
    case R0 of
        {ok, UUID} ->
            get_(Realm, UUID);
        R ->
            R
    end.

-spec get(Realm::binary(), Role::fifo:role_id()) ->
                 not_found |
                 {error, timeout} |
                 {ok, Role::fifo:role()}.
get(Realm, Role) ->
    case get_(Realm, Role) of
        {ok, RoleObj} ->
            {ok, ft_role:to_json(RoleObj)};
        R  ->
            R
    end.

-spec get_(Realm::binary(), Role::fifo:role_id()) ->
                  not_found |
                  {error, timeout} |
                  {ok, Role::#?ROLE{}}.
get_(Realm, Role) ->
    case ?FM(get, snarl_entity_read_fsm, start,
             [{snarl_role_vnode, snarl_role}, get, {Realm, Role}]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

raw(Realm, Role) ->
    case ?FM(get, snarl_entity_read_fsm, start,
             [{snarl_role_vnode, snarl_role}, get, {Realm, Role}, undefined, true]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

list() ->
    ?FM(list_all, snarl_coverage, start,
        [snarl_role_vnode_master, snarl_role, list]).

-spec list(Realm::binary()) -> {ok, [fifo:role_id()]} |
                not_found |
                {error, timeout}.

list(Realm) ->
    ?FM(list, snarl_coverage, start,
        [snarl_role_vnode_master, snarl_role, {list, Realm}]).

list_(Realm) ->
    {ok, Res} =
        ?FM(list, snarl_full_coverage, start,
            [snarl_role_vnode_master, snarl_role,
             {list, Realm, [], true, true}]),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.


%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list(Realm::binary(), [fifo:matcher()], boolean()) ->
                  {error, timeout} | {ok, [fifo:uuid()]}.

list(Realm, Requirements, Full)
  when Full == true orelse Full == false ->
    {ok, Res} =
        ?FM(list, snarl_full_coverage, start,
            [snarl_role_vnode_master, snarl_role,
             {list, Realm, Requirements, Full}]),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

-spec add(Realm::binary(), Role::binary()) ->
                 {ok, UUID::fifo:role_id()} |
                 douplicate |
                 {error, timeout}.

add(Realm, Role) ->
    UUID = uuid:uuid4s(),
    create(Realm, UUID, Role).

create(Realm, UUID, Role) ->
    case snarl_role:lookup_(Realm, Role) of
        not_found ->
            ok = do_write(Realm, UUID, add, Role),
            {ok, UUID};
        {ok, _RoleObj} ->
            duplicate
    end.

-spec delete(Realm::binary(), Role::fifo:role_id()) ->
                    ok |
                    not_found|
                    {error, timeout}.

delete(Realm, Role) ->
    Res = do_write(Realm, Role, delete),
    spawn(
      fun () ->
              Prefix = [<<"roles">>, Role],
              {ok, Users} = snarl_user:list(Realm),
              [begin
                   snarl_user:leave(Realm, U, Role),
                   snarl_user:revoke_prefix(Realm, U, Prefix)
               end
               || U <- Users],
              {ok, Roles} = snarl_role:list(Realm),
              [revoke_prefix(Realm, R, Prefix) || R <- Roles],
              {ok, Orgs} = snarl_org:list(Realm),
              [snarl_org:remove_target(Realm, O, Role) || O <- Orgs]
      end),
    Res.

-spec grant(Realm::binary(), Role::fifo:role_id(), fifo:permission()) ->
                   ok |
                   not_found|
                   {error, timeout}.

grant(Realm, Role, Permission) ->
    do_write(Realm, Role, grant, Permission).

-spec revoke(Realm::binary(),
             Role::fifo:role_id(), fifo:permission()) ->
                    ok |
                    not_found|
                    {error, timeout}.

revoke(Realm, Role, Permission) ->
    do_write(Realm, Role, revoke, Permission).

-spec revoke_prefix(Realm::binary(),
                    Role::fifo:role_id(), fifo:permission()) ->
                           ok |
                           not_found|
                           {error, timeout}.

revoke_prefix(Realm, Role, Prefix) ->
    do_write(Realm, Role, revoke_prefix, Prefix).

-spec set(Realm::binary(), Role::fifo:role_id(), Attirbute::fifo:key(),
          Value::fifo:value()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(Realm, Role, Attribute, Value) ->
    set(Realm, Role, [{Attribute, Value}]).

-spec set(Realm::binary(), Role::fifo:role_id(),
          Attirbutes::fifo:attr_list()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(Realm, Role, Attributes) ->
    do_write(Realm, Role, set, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Realm, Role, Op) ->
    ?FM(Op, snarl_entity_write_fsm, write,
        [{snarl_role_vnode, snarl_role}, {Realm, Role}, Op]).

do_write(Realm, Role, Op, Val) ->
    ?FM(Op, snarl_entity_write_fsm, write,
        [{snarl_role_vnode, snarl_role}, {Realm, Role}, Op, Val]).
