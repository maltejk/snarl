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
         metadata/1, set_metadata/3,
         merge/2,
         to_json/1,
         expire/2,
         gcable/1,
         gc/3
        ]).

-ignore_xref([
              new/0,
              load/1,
              uuid/1, uuid/3,
              name/1, name/3,
              triggers/1, add_trigger/3, remove_trigger/3,
              metadata/1, set_metadata/3,
              merge/2,
              to_json/1,
              expire/2,
              gcable/1,
              gc/3
             ]).

gcable(#?ORG{
           triggers = Triggers
          }) ->
    vorsetg:gcable(Triggers).

gc(_ID,
   Ps,
   #?ORG{
       triggers = Triggers
      } = Org) ->
    Ps1 = lists:foldl(fun vorsetg:gc/2, Triggers, Ps),
    Org#?ORG{
           triggers = Ps1
          }.

new() ->
    Size = ?ENV(org_bucket_size, 50),
    #?ORG{
        uuid = vlwwregister:new(<<>>),
        name = vlwwregister:new(<<>>),
        triggers = vorsetg:new(Size),
        metadata = statebox:new(fun jsxd:new/0)
       }.


load(#?ORG{} = Org) ->
    Org;

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
    #?ORG{
        uuid = vlwwregister:new(UUID),
        name = vlwwregister:new(Name),
        triggers = Triggers,
        metadata = statebox:new(fun () -> Metadata end)
       }.

jsonify_trigger({Trigger, Action}) ->
    jsxd:set(<<"trigger">>, Trigger, jsonify_action(Action)).

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
     {<<"Org">>, Org}];
jsonify_action({join, group, Group}) ->
    [{<<"action">>, <<"join_group">>},
     {<<"group">>, Group}].


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
       {<<"uuid">>, vlwwregister:value(UUID)},
       {<<"name">>, vlwwregister:value(Name)},
       {<<"triggers">>, [jsonify_trigger(T) || T <- vorsetg:value(Triggers)]},
       {<<"metadata">>, statebox:value(Metadata)}
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
        uuid = vlwwregister:merge(UUID1, UUID2),
        name = vlwwregister:merge(Name1, Name2),
        triggers = vorsetg:merge(Triggers1, Triggers2),
        metadata = statebox:merge([Metadata1, Metadata2])
       }.

name(Org) ->
    vlwwregister:value(Org#?ORG.name).

name(_, Name, Org) ->
    Org#?ORG{
           name = vlwwregister:assign(Name, Org#?ORG.name)
          }.

uuid(Org) ->
    vlwwregister:value(Org#?ORG.uuid).

uuid(_, UUID, Org) ->
    Org#?ORG{
           uuid = vlwwregister:assign(UUID, Org#?ORG.uuid)
          }.

triggers(Org) ->
    vorsetg:value(Org#?ORG.triggers).

add_trigger(ID, Trigger, Org) ->
    Org#?ORG{
           triggers =
               vorsetg:add(ID, Trigger, Org#?ORG.triggers)
          }.


remove_trigger(ID, Trigger, Org) ->
    Org#?ORG{
           triggers =
               vorsetg:remove(ID, Trigger, Org#?ORG.triggers)
          }.

metadata(Org) ->
    Org#?ORG.metadata.

set_metadata(Attribute, delete, Org) ->
    Org#?ORG{
           metadata = jsxd:delete(Attribute, Org#?ORG.metadata)
          };

set_metadata(Attribute, Value, Org) ->
    Org#?ORG{
           metadata = jsxd:set(Attribute, Value, Org#?ORG.metadata)
          }.

expire(Timeout, Org) ->
    Org#?ORG{
           metadata =
               statebox:expire(Timeout, Org#?ORG.metadata)
          }.

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
