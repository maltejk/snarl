%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(snarl_group_state).

-include("snarl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         new/0,
         load/1,
         uuid/1, uuid/3,
         name/1, name/3,
         permissions/1, grant/3, revoke/3, revoke_prefix/3,
         metadata/1, set_metadata/3,
         merge/2,
         to_json/1,
         expire/2
        ]).

-ignore_xref([
              new/0,
              load/1,
              uuid/1, uuid/2,
              name/1, name/2,
              permissions/1, grant/3, revoke/3, revoke_prefix/3,
              metadata/1, set_metadata/3,
              to_json/1
             ]).


new() ->
    #?GROUP{
        uuid = vlwwregister:new(<<>>),
        name = vlwwregister:new(<<>>),
        permissions = vorsetg:new(),
        metadata = statebox:new(fun jsxd:new/0)
       }.


load(#?GROUP{} = Group) ->
    Group;

load(Group) ->
    {ok, Name} = jsxd:get([<<"name">>], Group),
    {ok, UUID} = jsxd:get([<<"uuid">>], Group),
    ID0 = {{0,0,0}, load},
    {ok, Permissions0} = jsxd:get([<<"permissions">>], Group),
    {ok, Metadata} = jsxd:get([<<"metadata">>], Group),
    Permissions = lists:foldl(
                    fun (G, Acc) ->
                            vorsetg:add(ID0, G, Acc)
                    end, vorsetg:new(), Permissions0),
    #?GROUP{
        uuid = vlwwregister:new(UUID),
        name = vlwwregister:new(Name),
        permissions = Permissions,
        metadata = statebox:new(fun () -> Metadata end)
       }.

to_json(#?GROUP{
            uuid = UUID,
            name = Name,
            permissions = Permissions,
            metadata = Metadata
           }) ->
    jsxd:from_list(
      [
       {<<"uuid">>, vlwwregister:value(UUID)},
       {<<"name">>, vlwwregister:value(Name)},
       {<<"permissions">>, vorsetg:new(Permissions)},
       {<<"metadata">>, statebox:value(Metadata)}
      ]).

merge(#?GROUP{
            uuid = UUID1,
            name = Name1,
            permissions = Permissions1,
            metadata = Metadata1
         },
      #?GROUP{
          uuid = UUID2,
          name = Name2,
          permissions = Permissions2,
          metadata = Metadata2
         }) ->
    #?GROUP{
        uuid = vlwwregister:merge(UUID1, UUID2),
        name = vlwwregister:merge(Name1, Name2),
        permissions = vorsetg:merge(Permissions1, Permissions2),
        metadata = statebox:merge([Metadata1, Metadata2])
       }.

name(Group) ->
    vlwwregister:value(Group#?GROUP.name).

name(_, Name, Group) ->
    Group#?GROUP{
             name = vlwwregister:assign(Name, Group#?GROUP.name)
            }.

uuid(Group) ->
    vlwwregister:value(Group#?GROUP.uuid).

uuid(_, UUID, Group) ->
    Group#?GROUP{
             uuid = vlwwregister:assign(UUID, Group#?GROUP.uuid)
            }.

permissions(Group) ->
    vorsetg:value(Group#?GROUP.permissions).

grant(ID, Permission, Group) ->
    Group#?GROUP{
             permissions =
                 vorsetg:add(ID, Permission, Group#?GROUP.permissions)
            }.


revoke(ID, Permission, Group) ->
    Group#?GROUP{
             permissions =
                 vorsetg:remove(ID, Permission, Group#?GROUP.permissions)
            }.

revoke_prefix(ID, Prefix, Group) ->
    P0 = Group#?GROUP.permissions,
    Ps = permissions(Group),
    P1 = lists:foldl(fun (P, PAcc) ->
                             case lists:prefix(Prefix, P) of
                                 true ->
                                     vorsetg:remove(ID, P, PAcc);
                                 _ ->
                                     PAcc
                             end
                     end, P0, Ps),
    Group#?GROUP{
             permissions = P1
            }.

metadata(Group) ->
    Group#?GROUP.metadata.

set_metadata(Attribute, delete, Group) ->
    Group#?GROUP{
             metadata = jsxd:delete(Attribute, Group#?GROUP.metadata)
            };

set_metadata(Attribute, Value, Group) ->
    Group#?GROUP{
             metadata = jsxd:set(Attribute, Value, Group#?GROUP.metadata)
            }.

expire(Timeout, Group) ->
    Group#?GROUP{
            metadata =
                statebox:expire(Timeout, Group#?GROUP.metadata)
           }.

-ifdef(TEST).
reqid() ->
    {MegaSecs,Secs,MicroSecs} = erlang:now(),
	{(MegaSecs*1000000 + Secs)*1000000 + MicroSecs, test}.

name_test() ->
    Name0 = <<"Test0">>,
    Group0 = new(),
    Group1 = name(reqid(), Name0, Group0),
    Name1 = <<"Test1">>,
    Group2 = name(reqid(), Name1, Group1),
    ?assertEqual(Name0, name(Group1)),
    ?assertEqual(Name1, name(Group2)).

permissions_test() ->
    P0 = [<<"P0">>],
    P1 = [<<"P1">>],
    Group0 = new(),
    Group1 = grant(reqid(), P0, Group0),
    Group2 = grant(reqid(), P1, Group1),
    Group3 = grant(reqid(), P0, Group2),
    Group4 = revoke(reqid(), P0, Group3),
    Group5 = revoke(reqid(), P1, Group3),
    ?assertEqual([P0], permissions(Group1)),
    ?assertEqual([P0, P1], permissions(Group2)),
    ?assertEqual([P0, P1], permissions(Group3)),
    ?assertEqual([P1], permissions(Group4)),
    ?assertEqual([P0], permissions(Group5)).

-endif.
