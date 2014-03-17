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
         new/1,
         load/2,
         uuid/1, uuid/3,
         name/1, name/3,
         permissions/1, grant/3, revoke/3, revoke_prefix/3,
         metadata/1, set_metadata/4,
         merge/2,
         to_json/1,
         getter/2,
         is_a/1
        ]).

-export_type([group/0, any_group/0]).

-ignore_xref([
              new/1,
              load/2,
              uuid/1, uuid/2,
              name/1, name/2,
              permissions/1, grant/3, revoke/3, revoke_prefix/3,
              metadata/1, set_metadata/4,
              getter/2,
              to_json/1
             ]).

-type group() :: #?GROUP{}.

-type any_group() ::
        #group_0_1_0{} |
        #?GROUP{} |
        statebox:statebox().

getter(#snarl_obj{val=S0}, <<"uuid">>) ->
    ID = snarl_vnode:mkid(getter),
    uuid(load(ID, S0)).

is_a(#?GROUP{}) ->
    true;
is_a(_) ->
    false.


new({T, _ID}) ->
    {ok, UUID} = ?NEW_LWW(<<>>, T),
    {ok, Name} = ?NEW_LWW(<<>>, T),
    #?GROUP{
        uuid = UUID,
        name = Name,
        permissions = riak_dt_orswot:new(),
        metadata = snarl_map:new()
       }.

%%-spec load({integer(), atom()}, any_group()) -> group().

load(_, #?GROUP{} = Group) ->
    Group;

load({T, ID},
     #group_0_1_0{
        uuid = UUID,
        name = Name,
        permissions = Permissions,
        metadata = Metadata
       }) ->
    {ok, UUID1} = ?NEW_LWW(vlwwregister:value(UUID), T),
    {ok, Name1} = ?NEW_LWW(vlwwregister:value(Name), T),
    {ok, Permissions1} = ?CONVERT_VORSET(Permissions),
    Metadata1 = snarl_map:from_orddict(statebox:value(Metadata), ID, T),
    load({T, ID},
         #group_0_1_1{
            uuid = UUID1,
            name = Name1,
            permissions = Permissions1,
            metadata = Metadata1
           });

load(IDT, GroupSB) ->
    Size = ?ENV(group_bucket_size, 50),
    Group = statebox:value(GroupSB),
    {ok, Name} = jsxd:get([<<"name">>], Group),
    {ok, UUID} = jsxd:get([<<"uuid">>], Group),
    ID0 = {{0,0,0}, load},
    Permissions0 = jsxd:get([<<"permissions">>], [], Group),
    Metadata = jsxd:get([<<"metadata">>], [], Group),
    Permissions = lists:foldl(
                    fun (G, Acc) ->
                            vorsetg:add(ID0, G, Acc)
                    end, vorsetg:new(Size), Permissions0),
    load(IDT,
         #group_0_1_0{
            uuid = vlwwregister:new(UUID),
            name = vlwwregister:new(Name),
            permissions = Permissions,
            metadata = statebox:new(fun () -> Metadata end)
           }).

to_json(#?GROUP{
            uuid = UUID,
            name = Name,
            permissions = Permissions,
            metadata = Metadata
           }) ->
    jsxd:from_list(
      [
       {<<"uuid">>, riak_dt_lwwreg:value(UUID)},
       {<<"name">>, riak_dt_lwwreg:value(Name)},
       {<<"permissions">>, riak_dt_orswot:value(Permissions)},
       {<<"metadata">>, snarl_map:value(Metadata)}
      ]).

-spec merge(group(), group()) -> group().

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
        uuid = riak_dt_lwwreg:merge(UUID1, UUID2),
        name = riak_dt_lwwreg:merge(Name1, Name2),
        permissions = riak_dt_orswot:merge(Permissions1, Permissions2),
        metadata = snarl_map:merge(Metadata1, Metadata2)
       }.

name(Group) ->
    riak_dt_lwwreg:value(Group#?GROUP.name).

name({T, _ID}, Name, Group) ->
    {ok, V} = riak_dt_lwwreg:update({assign, Name, T}, none, Group#?GROUP.name),
    Group#?GROUP{name = V}.

uuid(Group) ->
    riak_dt_lwwreg:value(Group#?GROUP.uuid).

-spec uuid(Actor::term(), UUID::binary(), Group) ->
                  Group.
uuid({T, _ID}, UUID, Group = #?GROUP{}) ->
    {ok, V} = riak_dt_lwwreg:update({assign, UUID, T}, none, Group#?GROUP.uuid),
    Group#?GROUP{uuid = V}.

permissions(Group) ->
    riak_dt_orswot:value(Group#?GROUP.permissions).

grant({_T, ID}, Permission, Group = #?GROUP{}) ->
    {ok, V} = riak_dt_orswot:update({add, Permission},
                                    ID, Group#?GROUP.permissions),
    Group#?GROUP{permissions = V}.


revoke({_T, ID}, Permission, Group) ->
    {ok, V} =  riak_dt_orswot:update({remove, Permission},
                                     ID, Group#?GROUP.permissions),
    Group#?GROUP{permissions = V}.

revoke_prefix({_T, ID}, Prefix, Group) ->
    P0 = Group#?GROUP.permissions,
    Ps = permissions(Group),
    P1 = lists:foldl(fun (P, PAcc) ->
                             case lists:prefix(Prefix, P) of
                                 true ->
                                     {ok, V} =  riak_dt_orswot:update(
                                                  {remove, P}, ID, PAcc),
                                     V;
                                 _ ->
                                     PAcc
                             end
                     end, P0, Ps),
    Group#?GROUP{
             permissions = P1
            }.

metadata(Group) ->
    Group#?GROUP.metadata.

set_metadata({T, ID}, P, Value, Group) when is_binary(P) ->
    set_metadata({T, ID}, snarl_map:split_path(P), Value, Group);

set_metadata({_T, ID}, Attribute, delete, Group) ->
    {ok, M1} = snarl_map:remove(Attribute, ID, Group#?GROUP.metadata),
    Group#?GROUP{metadata = M1};

set_metadata({T, ID}, Attribute, Value, Group) ->
    {ok, M1} = snarl_map:set(Attribute, Value, ID, T, Group#?GROUP.metadata),
    Group#?GROUP{metadata = M1}.

-ifdef(TEST).
mkid() ->
    {ecrdt:timestamp_us(), test}.

to_json_test() ->
    Group = new(mkid()),
    GroupJ = [{<<"metadata">>,[]},
              {<<"name">>,<<>>},
              {<<"permissions">>,[]},
              {<<"uuid">>,<<>>}],
    ?assertEqual(GroupJ, to_json(Group)).

name_test() ->
    Name0 = <<"Test0">>,
    Group0 = new(mkid()),
    Group1 = name(mkid(), Name0, Group0),
    Name1 = <<"Test1">>,
    Group2 = name(mkid(), Name1, Group1),
    ?assertEqual(Name0, name(Group1)),
    ?assertEqual(Name1, name(Group2)).

permissions_test() ->
    P0 = [<<"P0">>],
    P1 = [<<"P1">>],
    Group0 = new(mkid()),
    Group1 = grant(mkid(), P0, Group0),
    Group2 = grant(mkid(), P1, Group1),
    Group3 = grant(mkid(), P0, Group2),
    Group4 = revoke(mkid(), P0, Group3),
    Group5 = revoke(mkid(), P1, Group3),
    ?assertEqual([P0], permissions(Group1)),
    ?assertEqual([P0, P1], permissions(Group2)),
    ?assertEqual([P0, P1], permissions(Group3)),
    ?assertEqual([P1], permissions(Group4)),
    ?assertEqual([P0], permissions(Group5)).

-endif.
