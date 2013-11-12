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
         new/0,
         load/1,
         uuid/1, uuid/3,
         name/1, name/3,
         triggers/1, add_trigger/3, remove_trigger/3,
         metadata/1, set_metadata/4,
         merge/2,
         to_json/1,
         is_a/1
        ]).

-export_type([organisation/0, any_organisation/0]).

-ignore_xref([
              new/0,
              load/1,
              uuid/1, uuid/3,
              name/1, name/3,
              triggers/1, add_trigger/3, remove_trigger/3,
              metadata/1, set_metadata/4,
              merge/2,
              to_json/1
             ]).

-opaque organisation() :: #?ORG{}.

-opaque any_organisation() :: organisation() |
                              #organisation_0_1_0{} |
                              statebox:statebox().

is_a(#?ORG{}) ->
    true;
is_a(_) ->
    false.

new() ->
    {ok, UUID} = ?NEW_LWW(<<>>),
    {ok, Name} = ?NEW_LWW(<<>>),
    #?ORG{
        uuid = UUID,
        name = Name,
        triggers = riak_dt_orswot:new(),
        metadata = snarl_map:new()
       }.

-spec load(any_organisation()) -> organisation().

load(#?ORG{} = Org) ->
    Org;

load(#organisation_0_1_0{
        uuid = UUID,
        name = Name,
        triggers = Triggers,
        metadata = Metadata
       }) ->
    {ok, UUID1} = ?NEW_LWW(vlwwregister:value(UUID)),
    {ok, Name1} = ?NEW_LWW(vlwwregister:value(Name)),
    {ok, Triggers1} = ?CONVERT_VORSET(Triggers),
    Metadata1 = snarl_map:from_orddict(statebox:value(Metadata), none),
    load(#organisation_0_1_1{
            uuid = UUID1,
            name = Name1,
            triggers = Triggers1,
            metadata = Metadata1
           });

load(OrgSB) ->
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
    #organisation_0_1_0{
        uuid = vlwwregister:new(UUID),
        name = vlwwregister:new(Name),
        triggers = Triggers,
        metadata = statebox:new(fun () -> Metadata end)
       }.

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
       {<<"triggers">>, [jsonify_trigger(T) || T <- riak_dt_orswot:value(Triggers)]},
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
        triggers = riak_dt_orswot:merge(Triggers1, Triggers2),
        metadata = snarl_map:merge(Metadata1, Metadata2)
       }.

name(Org) ->
    riak_dt_lwwreg:value(Org#?ORG.name).

name(_, Name, Org) ->
    {ok, V} = riak_dt_lwwreg:update({assign,Name}, none, Org#?ORG.name),
    Org#?ORG{name = V}.

uuid(Org) ->
    riak_dt_lwwreg:value(Org#?ORG.uuid).

uuid(_, UUID, Org) ->
    {ok, V} = riak_dt_lwwreg:update({assign, UUID}, none, Org#?ORG.uuid),
    Org#?ORG{uuid = V}.

triggers(Org) ->
    riak_dt_orswot:value(Org#?ORG.triggers).

add_trigger(ID, Trigger, Org) ->
    {ok, V} = riak_dt_orswot:update({add, Trigger}, ID, Org#?ORG.triggers),
    Org#?ORG{triggers = V}.


remove_trigger(ID, Trigger, Org) ->
    {ok, V} = riak_dt_orswot:update({remove, Trigger}, ID, Org#?ORG.triggers),
    Org#?ORG{triggers = V}.

metadata(Org) ->
    Org#?ORG.metadata.

set_metadata(ID, P, Value, Org) when is_binary(P) ->
    set_metadata(ID, snarl_map:split_path(P), Value, Org);

set_metadata(ID, Attribute, delete, Org) ->
    {ok, M1} = snarl_map:remove(Attribute, ID, Org#?ORG.metadata),
    Org#?ORG{metadata = M1};

set_metadata(ID, Attribute, Value, Org) ->
    {ok, M1} = snarl_map:set(Attribute, Value, ID, Org#?ORG.metadata),
    Org#?ORG{metadata = M1}.

-ifdef(TEST).
mkid() ->
    {ecrdt:timestamp_us(), test}.

to_json_test() ->
    Org = new(),
    OrgJ = [{<<"metadata">>,[]},
            {<<"name">>,<<>>},
            {<<"triggers">>,[]},
            {<<"uuid">>,<<>>}],
    ?assertEqual(OrgJ, to_json(Org)).

name_test() ->
    Name0 = <<"Test0">>,
    Org0 = new(),
    Org1 = name(mkid(), Name0, Org0),
    Name1 = <<"Test1">>,
    Org2 = name(mkid(), Name1, Org1),
    ?assertEqual(Name0, name(Org1)),
    ?assertEqual(Name1, name(Org2)).

triggers_test() ->
    ok.

-endif.
