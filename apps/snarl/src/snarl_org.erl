-module(snarl_org).
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         sync_repair/3,
         list/0, list/1, list_/1, list/3,
         get/2, raw/2, lookup/2,
         add/2,
         delete/2,
         set/3, set/4,
         create/3,
         import/3,
         trigger/4,
         add_trigger/3, remove_trigger/3,
         remove_target/3,
         wipe/2
        ]).

-ignore_xref([
              import/3,
              lookup/2,
              create/3,
              import/3,
              list_/1,
              raw/2,
              sync_repair/3,
              wipe/2
             ]).

-define(TIMEOUT, 5000).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {snarl, org, Met},
          Mod, Fun, Args)).

-type template() :: [binary()|placeholder].
%% Public API

wipe(Realm, UUID) ->
    ?FM(wipe, snarl_coverage, start,
        [snarl_org_vnode_master, snarl_org, {wipe, Realm, UUID}]).

sync_repair(Realm, UUID, Obj) ->
    do_write(Realm, UUID, sync_repair, Obj).

add_trigger(Realm, Org, Trigger) ->
    do_write(Realm, Org, add_trigger, {uuid:uuid4s(), Trigger}).

remove_target(Realm, Org, Target) ->
    do_write(Realm, Org, remove_target, Target).

remove_trigger(Realm, Org, Trigger) ->
    do_write(Realm, Org, remove_trigger, Trigger).

trigger(Realm, Org, Event, Payload) ->
    case snarl_org:get(Realm, Org) of
        {ok, OrgObj} ->
            Triggers = [T || {_, T} <- ft_org:triggers(OrgObj)],
            Executed = do_events(Realm, Triggers, Event, Payload, 0),
            {ok, Executed};
        R  ->
            R
    end.

do_events(Realm, [{Event, Template}|Ts], Event, Payload, N) ->
    do_event(Realm, Template, Payload),
    do_events(Realm, Ts, Event, Payload, N+1);

do_events(Realm, [_|Ts], Event, Payload, N) ->
    do_events(Realm, Ts, Event, Payload, N);

do_events(_Realm, [], _Event, _Payload, N) ->
    N.

-spec do_event(Realm::binary(),
               Action::{grant, role, Role::fifo:role_id(), Template::template()} |
                       {grant, user, User::fifo:user_id(), Template::template()} |
                       {join, role, Role::fifo:role_id()} |
                       {join, org, Org::fifo:org_id()},
               Payload::template()) ->
                      ok.

do_event(Realm, {join, role, Role}, Payload) ->
    snarl_user:join(Realm, Payload, Role),
    ok;

do_event(Realm, {join, org, Org}, Payload) ->
    snarl_user:join_org(Realm, Payload, Org),
    snarl_user:select_org(Realm, Payload, Org),
    ok;

do_event(Realm, {grant, role, Role, Template}, Payload) ->
    snarl_role:grant(Realm, Role, build_template(Template, Payload)),
    ok;

do_event(Realm, {grant, user, Role, Template}, Payload) ->
    snarl_user:grant(Realm, Role, build_template(Template, Payload)),
    ok.

build_template(Template, Payload) ->
    lists:map(fun(placeholder) ->
                      Payload;
                 (E) ->
                      E
              end, Template).


import(Realm, Org, Data) ->
    do_write(Realm, Org, import, Data).

-spec lookup(Realm::binary(), OrgName::binary()) ->
                    not_found |
                    {error, timeout} |
                    {ok, Org::fifo:org()}.

lookup(Realm, OrgName) ->
    {ok, Res} =
        ?FM(lookup, snarl_coverage, start,
            [snarl_org_vnode_master, snarl_org, {lookup, Realm, OrgName}]),
    R0 = lists:foldl(fun (not_found, Acc) ->
                             Acc;
                         (R, _) ->
                             {ok, R}
                     end, not_found, Res),
    case R0 of
        {ok, UUID} ->
            snarl_org:get(Realm, UUID);
        R ->
            R
    end.

-spec get(Realm::binary(), Org::fifo:org_id()) ->
                  not_found |
                  {error, timeout} |
                  {ok, Org::fifo:org()}.
get(Realm, Org) ->
    case ?FM(get, snarl_entity_read_fsm, start,
             [{snarl_org_vnode, snarl_org}, get, {Realm, Org}]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

raw(Realm, Org) ->
    case ?FM(get, snarl_entity_read_fsm, start,
             [{snarl_org_vnode, snarl_org}, get,
              {Realm, Org}, undefined, true]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

list_(Realm) ->
    {ok, Res} =
        ?FM(list, snarl_full_coverage, start,
            [snarl_org_vnode_master, snarl_org, {list, Realm, [], true, true}]),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.

-spec list(Realm::binary()) -> {ok, [fifo:org_id()]} |
                               not_found |
                               {error, timeout}.

list(Realm) ->
    ?FM(list, snarl_coverage, start,
        [snarl_org_vnode_master, snarl_org, {list, Realm}]).

list() ->
    ?FM(list_all, snarl_coverage, start,
        [snarl_org_vnode_master, snarl_org, list]).

-spec list(Realm::binary(), [fifo:matcher()], boolean()) ->
                  {error, timeout} | {ok, [fifo:uuid()]}.

list(Realm, Requirements, Full)
  when Full == true orelse Full == false ->
    {ok, Res} =
        ?FM(list, snarl_full_coverage, start,
            [snarl_org_vnode_master, snarl_org,
             {list, Realm, Requirements, Full}]),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.

-spec add(Realm::binary(), Org::binary()) ->
                 {ok, UUID::fifo:org_id()} |
                 douplicate |
                 {error, timeout}.

add(Realm, Org) ->
    UUID = uuid:uuid4s(),
    create(Realm, UUID, Org).

create(Realm, UUID, Org) ->
    case snarl_org:lookup(Realm, Org) of
        not_found ->
            ok = do_write(Realm, UUID, add, Org),
            {ok, UUID};
        {ok, _OrgObj} ->
            duplicate
    end.

-spec delete(Realm::binary(), Org::fifo:org_id()) ->
                    ok |
                    not_found|
                    {error, timeout}.

delete(Realm, Org) ->
    Res = do_write(Realm, Org, delete),
    spawn(
      fun () ->
              Prefix = [<<"orgs">>, Org],
              {ok, Users} = snarl_user:list(Realm),
              [begin
                   snarl_user:leave_org(Realm, U, Org),
                   snarl_user:revoke_prefix(Realm, U, Prefix)
               end || U <- Users],
              {ok, Roles} = snarl_role:list(Realm),
              [snarl_role:revoke_prefix(Realm, R, Prefix) || R <- Roles],
              {ok, Orgs} = snarl_org:list(Realm),
              [snarl_org:remove_target(Realm, O, Org) || O <- Orgs]
      end),
    Res.


-spec set(Realm::binary(), Org::fifo:org_id(), Attirbute::fifo:key(), Value::fifo:value()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(Realm, Org, Attribute, Value) ->
    set(Realm, Org, [{Attribute, Value}]).

-spec set(Realm::binary(), Org::fifo:org_id(), Attirbutes::fifo:attr_list()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(Realm, Org, Attributes) ->
    do_write(Realm, Org, set, Attributes).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Realm, Org, Op) ->
    ?FM(Op, snarl_entity_write_fsm, write,
        [{snarl_org_vnode, snarl_org}, {Realm, Org}, Op]).

do_write(Realm, Org, Op, Val) ->
    ?FM(Op, snarl_entity_write_fsm, write,
        [{snarl_org_vnode, snarl_org}, {Realm, Org}, Op, Val]).
