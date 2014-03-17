%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(snarl_org_state).

-include("snarl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         new/1,
         load/2,
         uuid/1, uuid/3,
         name/1, name/3,
         triggers/1, add_trigger/4, remove_trigger/3,
         metadata/1, set_metadata/4,
         merge/2,
         to_json/1,
         getter/2,
         is_a/1
        ]).

-export_type([organisation/0, any_organisation/0]).

-ignore_xref([
              new/1,
              load/2,
              uuid/1, uuid/3,
              name/1, name/3,
              triggers/1, add_trigger/4, remove_trigger/3,
              metadata/1, set_metadata/4,
              merge/2,
              to_json/1,
              getter/2,
              is_a/1
             ]).

-type organisation() :: #?ORG{}.

-type any_organisation() :: organisation() |
                            #organisation_0_1_0{} |
                            #organisation_0_1_1{} |
                            #organisation_0_1_2{} |
                            statebox:statebox().

getter(#snarl_obj{val=S0}, <<"uuid">>) ->
    ID = snarl_vnode:mkid(getter),
    uuid(snarl_org_state:load(ID, S0)).

is_a(#?ORG{}) ->
    true;
is_a(_) ->
    false.

new({T, _ID}) ->
    {ok, UUID} = ?NEW_LWW(<<>>, T),
    {ok, Name} = ?NEW_LWW(<<>>, T),
    #?ORG{
        uuid = UUID,
        name = Name,
        triggers = snarl_map:new(),
        metadata = snarl_map:new()
       }.

%%-spec load({non_neg_integer(), atom()}, any_organisation()) -> organisation().

load(_, #?ORG{} = Org) ->
    Org;

load({T, ID},
     #organisation_0_1_1{
        uuid = UUID,
        name = Name,
        triggers = Triggers,
        metadata = Metadata
       }) ->
    UUIDb = riak_dt_lwwreg:value(UUID),
    Ts = [{trigger_uuid(UUIDb, Tr), Tr} || Tr <- riak_dt_orswot:value(Triggers)],
    Triggers1 = snarl_map:from_orddict(orddict:from_list(Ts), ID, T),
    load({T, ID},
         #organisation_0_1_2{
            uuid = UUID,
            name = Name,
            triggers = Triggers1,
            metadata = Metadata
           });

load({T, ID},
     #organisation_0_1_0{
        uuid = UUID,
        name = Name,
        triggers = Triggers,
        metadata = Metadata
       }) ->
    {ok, UUID1} = ?NEW_LWW(vlwwregister:value(UUID), T),
    {ok, Name1} = ?NEW_LWW(vlwwregister:value(Name), T),
    {ok, Triggers1} = ?CONVERT_VORSET(Triggers),
    Metadata1 = snarl_map:from_orddict(statebox:value(Metadata), ID, T),
    load({T, ID},
         #organisation_0_1_1{
            uuid = UUID1,
            name = Name1,
            triggers = Triggers1,
            metadata = Metadata1
           });

load({T, ID}, OrgSB) ->
    Size = ?ENV(org_bucket_size, 50),
    Org = statebox:value(OrgSB),
    {ok, Name} = jsxd:get([<<"name">>], Org),
    {ok, UUID} = jsxd:get([<<"uuid">>], Org),
    ID0 = {{0,0,0}, load},
    Triggers0 = jsxd:get([<<"triggers">>], [], Org),
    Metadata = jsxd:get([<<"metadata">>], [], Org),
    Triggers = lists:foldl(
                 fun (G, Acc) ->
                         vorsetg:add(ID0, G, Acc)
                 end, vorsetg:new(Size), Triggers0),
    load({T, ID},
         #organisation_0_1_0{
            uuid = vlwwregister:new(UUID),
            name = vlwwregister:new(Name),
            triggers = Triggers,
            metadata = statebox:new(fun () -> Metadata end)
           }).

jsonify_trigger({Trigger, Action}) ->
    jsxd:set(<<"trigger">>, list_to_binary(atom_to_list(Trigger)),
             jsonify_action(Action)).

jsonify_action({grant, group, Target, Permission}) ->
    [{<<"action">>, <<"group_grant">>},
     {<<"permission">>, jsonify_permission(Permission)},
     {<<"target">>, Target}];

jsonify_action({grant, user, Target, Permission}) ->
    [{<<"action">>, <<"user_grant">>},
     {<<"permission">>, jsonify_permission(Permission)},
     {<<"target">>, Target}];

jsonify_action({join, org, Org}) ->
    [{<<"action">>, <<"join_org">>},
     {<<"target">>, Org}];

jsonify_action({join, group, Group}) ->
    [{<<"action">>, <<"join_group">>},
     {<<"target">>, Group}].


jsonify_permission(Permission) ->
    lists:map(fun (placeholder) ->
                      <<"$">>;
                  (E) ->
                      E
              end, Permission).

-spec to_json(Org::organisation()) -> fifo:org().
to_json(#?ORG{
            uuid = UUID,
            name = Name,
            triggers = Triggers,
            metadata = Metadata
           }) ->
    jsxd:from_list(
      [
       {<<"uuid">>, riak_dt_lwwreg:value(UUID)},
       {<<"name">>, riak_dt_lwwreg:value(Name)},
       {<<"triggers">>, [{U, jsonify_trigger(T)} || {U, T} <- snarl_map:value(Triggers)]},
       {<<"metadata">>, snarl_map:value(Metadata)}
      ]).

merge(#?ORG{
          uuid = UUID1,
          name = Name1,
          triggers = Triggers1,
          metadata = Metadata1
         },
      #?ORG{
          uuid = UUID2,
          name = Name2,
          triggers = Triggers2,
          metadata = Metadata2
         }) ->
    #?ORG{
        uuid = riak_dt_lwwreg:merge(UUID1, UUID2),
        name = riak_dt_lwwreg:merge(Name1, Name2),
        triggers = snarl_map:merge(Triggers1, Triggers2),
        metadata = snarl_map:merge(Metadata1, Metadata2)
       }.

name(Org) ->
    riak_dt_lwwreg:value(Org#?ORG.name).

name({T, _ID}, Name, Org) ->
    {ok, V} = riak_dt_lwwreg:update({assign, Name, T}, none, Org#?ORG.name),
    Org#?ORG{name = V}.

uuid(Org) ->
    riak_dt_lwwreg:value(Org#?ORG.uuid).

uuid({T, _ID}, UUID, Org) ->
    {ok, V} = riak_dt_lwwreg:update({assign, UUID, T}, none, Org#?ORG.uuid),
    Org#?ORG{uuid = V}.

-spec triggers(Org::organisation()) -> [{ID::fifo:uuid(), Trigger::term()}].

triggers(Org) ->
    snarl_map:value(Org#?ORG.triggers).

add_trigger({T, ID}, UUID, Trigger, Org) ->
    {ok, T1} = snarl_map:set(UUID, {reg, Trigger}, ID, T, Org#?ORG.triggers),
    Org#?ORG{triggers = T1}.

remove_trigger({_T, ID}, Trigger, Org) ->
    {ok, V} = snarl_map:remove(Trigger, ID, Org#?ORG.triggers),
    Org#?ORG{triggers = V}.

metadata(Org) ->
    Org#?ORG.metadata.

set_metadata({T, ID}, P, Value, Org) when is_binary(P) ->
    set_metadata({T, ID}, snarl_map:split_path(P), Value, Org);

set_metadata({_T, ID}, Attribute, delete, Org) ->
    {ok, M1} = snarl_map:remove(Attribute, ID, Org#?ORG.metadata),
    Org#?ORG{metadata = M1};

set_metadata({T, ID}, Attribute, Value, Org) ->
    {ok, M1} = snarl_map:set(Attribute, Value, ID, T, Org#?ORG.metadata),
    Org#?ORG{metadata = M1}.

-spec trigger_uuid(UUID::uuid:uuid_string(), Trigger::term()) -> uuid:uuid().

trigger_uuid(UUID, Trigger) ->
    list_to_binary(uuid:to_string(uuid:uuid5(UUID, term_to_binary(Trigger)))).

-ifdef(TEST).
mkid() ->
    {ecrdt:timestamp_us(), test}.

to_json_test() ->
    Org = new(mkid()),
    OrgJ = [{<<"metadata">>,[]},
            {<<"name">>,<<>>},
            {<<"triggers">>,[]},
            {<<"uuid">>,<<>>}],
    ?assertEqual(OrgJ, to_json(Org)).

name_test() ->
    Name0 = <<"Test0">>,
    Org0 = new(mkid()),
    Org1 = name(mkid(), Name0, Org0),
    Name1 = <<"Test1">>,
    Org2 = name(mkid(), Name1, Org1),
    ?assertEqual(Name0, name(Org1)),
    ?assertEqual(Name1, name(Org2)).

-endif.
