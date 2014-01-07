-module(snarl_org).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         sync_repair/2,
         ping/0,
         list/0,
         list/1,
         get/1,
         get_/1,
         raw/1,
         lookup/1,
         add/1,
         delete/1,
         set/2,
         set/3,
         create/2,
         import/2,
         trigger/3,
         add_trigger/2, remove_trigger/2
        ]).

-ignore_xref([
              ping/0,
              list/0,
              get/1,
              get_/1,
              lookup/1,
              add/1,
              delete/1,
              set/2,
              set/3,
              create/2,
              import/2,
              trigger/3,
              add_trigger/2, remove_trigger/2, raw/1, sync_repair/2
             ]).

-ignore_xref([ping/0, create/2]).

-define(TIMEOUT, 5000).

-type template() :: [binary()|placeholder].
%% Public API

sync_repair(UUID, Obj) ->
    do_write(UUID, sync_repair, Obj).

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, snarl_org),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, snarl_org_vnode_master).


add_trigger(Org, Trigger) ->
    do_write(Org, add_trigger, Trigger).

remove_trigger(Org, Trigger) ->
    do_write(Org, remove_trigger, Trigger).

trigger(Org, Event, Payload) ->
    case get_(Org) of
        {ok, OrgObj} ->
            Triggers = snarl_org_state:triggers(OrgObj),
            Executed = do_events(Triggers, Event, Payload, 0),
            {ok, Executed};
        R  ->
            R
    end.

do_events([{Event, Template}|Ts], Event, Payload, N) ->
    do_event(Template, Payload),
    do_events(Ts, Event, Payload, N+1);

do_events([_|Ts], Event, Payload, N) ->
    do_events(Ts, Event, Payload, N);

do_events([], _Event, _Payload, N) ->
    N.

-spec do_event(Action::{grant, group, Group::fifo:group_id(), Template::template()} |
                       {grant, user, User::fifo:user_id(), Template::template()} |
                       {join, group, Group::fifo:group_id()} |
                       {join, org, Org::fifo:org_id()},
               Payload::template()) ->
                      ok.

do_event({join, group, Group}, Payload) ->
    snarl_user:join(Payload, Group),
    ok;

do_event({join, org, Org}, Payload) ->
    snarl_user:join_org(Payload, Org),
    snarl_user:select_org(Payload, Org),
    ok;

do_event({grant, group, Group, Template}, Payload) ->
    snarl_group:grant(Group, build_template(Template, Payload)),
    ok;

do_event({grant, user, Group, Template}, Payload) ->
    snarl_user:grant(Group, build_template(Template, Payload)),
    ok.

build_template(Template, Payload) ->
    lists:map(fun(placeholder) ->
                      Payload;
                 (E) ->
                      E
              end, Template).


import(Org, Data) ->
    do_write(Org, import, Data).

-spec lookup(OrgName::binary()) ->
                    not_found |
                    {error, timeout} |
                    {ok, Org::fifo:org()}.

lookup(OrgName) ->
    {ok, Res} = snarl_coverage:start(
                  snarl_org_vnode_master, snarl_org,
                  {lookup, OrgName}),
    R0 = lists:foldl(fun (not_found, Acc) ->
                             Acc;
                         (R, _) ->
                             {ok, R}
                     end, not_found, Res),
    case R0 of
        {ok, UUID} ->
            snarl_org:get(UUID);
        R ->
            R
    end.

-spec get(Org::fifo:org_id()) ->
                 not_found |
                 {error, timeout} |
                 {ok, Org::fifo:org()}.
get(Org) ->
    case get_(Org) of
        {ok, OrgObj} ->
            {ok, snarl_org_state:to_json(OrgObj)};
        R  ->
            R
    end.

-spec get_(Org::fifo:org_id()) ->
                  not_found |
                  {error, timeout} |
                  {ok, Org::snarl_org_state:organisation()}.
get_(Org) ->
    case snarl_entity_read_fsm:start(
           {snarl_org_vnode, snarl_org},
           get, Org
          ) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

raw(Org) ->
    snarl_entity_read_fsm:start({snarl_org_vnode, snarl_org}, get,
                                Org, undefined, true).

-spec list() -> {ok, [fifo:org_id()]} |
                not_found |
                {error, timeout}.

list() ->
    snarl_coverage:start(
      snarl_org_vnode_master, snarl_org,
      list).

-spec list(Reqs::[fifo:matcher()]) ->
                  {ok, [IPR::fifo:group_id()]} | {error, timeout}.
list(Requirements) ->
    {ok, Res} = snarl_coverage:start(
                  snarl_org_vnode_master, snarl_org,
                  {list, Requirements}),
    Res1 = rankmatcher:apply_scales(Res),
    {ok,  lists:sort(Res1)}.


-spec add(Org::binary()) ->
                 {ok, UUID::fifo:org_id()} |
                 douplicate |
                 {error, timeout}.

add(Org) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    create(UUID, Org).

create(UUID, Org) ->
    case snarl_org:lookup(Org) of
        not_found ->
            ok = do_write(UUID, add, Org),
            {ok, UUID};
        {ok, _OrgObj} ->
            duplicate
    end.

-spec delete(Org::fifo:org_id()) ->
                    ok |
                    not_found|
                    {error, timeout}.

delete(Org) ->
    do_write(Org, delete).

-spec set(Org::fifo:org_id(), Attirbute::fifo:key(), Value::fifo:value()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(Org, Attribute, Value) ->
    set(Org, [{Attribute, Value}]).

-spec set(Org::fifo:org_id(), Attirbutes::fifo:attr_list()) ->
                 not_found |
                 {error, timeout} |
                 ok.
set(Org, Attributes) ->
    do_write(Org, set, Attributes).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Org, Op) ->
    snarl_entity_write_fsm:write({snarl_org_vnode, snarl_org}, Org, Op).

do_write(Org, Op, Val) ->
    snarl_entity_write_fsm:write({snarl_org_vnode, snarl_org}, Org, Op, Val).
