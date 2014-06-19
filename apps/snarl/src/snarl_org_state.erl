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
-export([jsonify_trigger/1]).
-endif.

-export([
         new/1,
         load/2,
         uuid/1, uuid/3,
         name/1, name/3,
         triggers/1, add_trigger/4, remove_trigger/3,
         metadata/1, set_metadata/4,
         remove_target/3,
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
              remove_target/3,
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

load(TID,
     #organisation_0_1_2{
        uuid = UUID,
        name = Name,
        triggers = Triggers,
        metadata = Metadata
       }) ->
    O = #organisation_0_1_3{
           uuid = UUID,
           name = Name,
           triggers = Triggers,
           metadata = Metadata
          },
    load(TID, update_triggers(TID, O));

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

jsonify_action({grant, role, Target, Permission}) ->
    [{<<"action">>, <<"role_grant">>},
     {<<"permission">>, jsonify_permission(Permission)},
     {<<"target">>, Target}];

jsonify_action({grant, user, Target, Permission}) ->
    [{<<"action">>, <<"user_grant">>},
     {<<"permission">>, jsonify_permission(Permission)},
     {<<"target">>, Target}];

jsonify_action({join, org, Org}) ->
    [{<<"action">>, <<"join_org">>},
     {<<"target">>, Org}];

jsonify_action({join, role, Role}) ->
    [{<<"action">>, <<"join_role">>},
     {<<"target">>, Role}].


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


remove_target(TID, Target, Org) ->
    Triggers = triggers(Org),
    GrantTriggers = [UUID || {UUID, {_, {grant, _, T, _}}} <- Triggers,
                             T =:= Target],
    JoinTriggers = [UUID || {UUID, {_, {join, _, T}}} <- Triggers,
                            T =:= Target],
    lists:foldl(fun(UUID, Acc) ->
                        remove_trigger(TID, UUID, Acc)
                end, Org, GrantTriggers ++ JoinTriggers).


update_triggers(TID, Org) ->
    lists:foldl(fun ({UUID, {A, {grant, group, R, T}}}, Acc) ->
                        Acc1 = remove_trigger(TID, UUID, Acc),
                        add_trigger(TID, UUID, {A, {grant, role, R, replace_group(T)}}, Acc1);
                    ({UUID, {A, {grant, E, R, T}}}, Acc) ->
                        Acc1 = remove_trigger(TID, UUID, Acc),
                        add_trigger(TID, UUID, {A, {grant, E, R, replace_group(T)}}, Acc1);
                    ({UUID, {A, {join, group, R}}}, Acc) ->
                        Acc1 = remove_trigger(TID, UUID, Acc),
                        add_trigger(TID, UUID, {A, {join, role, R}}, Acc1);
                    (_, Acc) ->
                        Acc
                end, Org, triggers(Org)).

replace_group(R) ->
    replace_group(R, []).

replace_group([<<"groups">> | R], Acc) ->
    replace_group(R, [<<"roles">> | Acc]);

replace_group([F | R], Acc) ->
    replace_group(R, [F | Acc]);

replace_group([], Acc) ->
    lists:reverse(Acc).



-ifdef(TEST).
mkid() ->
    {ecrdt:timestamp_us(), test}.

trigger_update_test() ->
    O = new(mkid()),
    %% Initialize a (old) trigger and the expected new one.
    U1 = uuid:uuid4s(),
    T1 = {vm_create, {grant, group, <<"bla">>,
                      [<<"groups">>, '$', <<"grant">>]}},
    T1u = {vm_create, {grant, role, <<"bla">>,
                       [<<"roles">>, '$', <<"grant">>]}},


    %% Create a org and add the trigger, test that the trigger is there
    O1 = add_trigger(mkid(), U1, T1, O),
    Ts = lists:sort([{U1, T1}]),
    RTs = lists:sort(triggers(O1)),
    ?assertEqual(Ts, RTs),

    %% Now update the trigger and compare the results
    O2 = update_triggers(mkid(), O1),
    Tsu = lists:sort([{U1, T1u}]),
    RTsu = lists:sort(triggers(O2)),
    ?assertEqual(Tsu, RTsu),

    %% Prepare a secdont trigger and add it to the unchanged org
    U2 = uuid:uuid4s(),
    T2 = {vm_create, {join, group, <<"bla">>}},
    T2u = {vm_create, {join, role, <<"bla">>}},
    O3 = add_trigger(mkid(), U2, T2, O1),

    %% Make sure both triggers are there.
    Ts1 = lists:sort([{U1, T1}, {U2, T2}]),
    RTs1 = lists:sort(triggers(O3)),
    ?assertEqual(Ts1, RTs1),

    %% Then update and check both triggers were updated correctly
    O4 = update_triggers(mkid(), O3),
    Tsu1 = lists:sort([{U1, T1u}, {U2, T2u}]),
    RTsu1 = lists:sort(triggers(O4)),
    ?assertEqual(Tsu1, RTsu1).


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


remove_target_test() ->
    Org0 = new(mkid()),

    %% Add a grant trigger.
    Target1 = uuid:uuid4s(),
    UUID1 = uuid:uuid4s(),
    Tr1 = {vm_create, {grant, group, Target1, a}},
    Org1 = add_trigger(mkid(), UUID1, Tr1, Org0),

    %% Add a join trigger
    Target2 = uuid:uuid4s(),
    UUID2 = uuid:uuid4s(),
    Tr2 = {vm_create, {join, group, Target2}},
    Org2 = add_trigger(mkid(), UUID2, Tr2, Org1),

    %% Test if both triggers are added
    Ts1 = lists:sort([{UUID1, Tr1}, {UUID2, Tr2}]),
    ResTs1 = lists:sort(triggers(Org2)),
    ?assertEqual(Ts1, ResTs1),

    %% Remove the first trigger
    Org3 = remove_target(mkid(), Target1, Org2),
    Ts2 = lists:sort([{UUID2, Tr2}]),
    ResTs2 = lists:sort(triggers(Org3)),
    ?assertEqual(Ts2, ResTs2),

    %% Remove the seconds trigger
    Org4 = remove_target(mkid(), Target2, Org3),
    Ts3 = [],
    ResTs3 = lists:sort(triggers(Org4)),
    ?assertEqual(Ts3, ResTs3),

    ok.


-endif.
