%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(snarl_role_state).

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

-export_type([role/0, any_role/0]).

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

-type role() :: #?ROLE{}.

-type any_role() ::
        #?ROLE{} |
        #group_0_1_1{} |
        #group_0_1_0{} |
        statebox:statebox().

getter(#snarl_obj{val=S0}, <<"uuid">>) ->
    ID = snarl_vnode:mkid(getter),
    uuid(load(ID, S0)).

is_a(#?ROLE{}) ->
    true;
is_a(_) ->
    false.


new({T, _ID}) ->
    {ok, UUID} = ?NEW_LWW(<<>>, T),
    {ok, Name} = ?NEW_LWW(<<>>, T),
    #?ROLE{
        uuid = UUID,
        name = Name,
        permissions = riak_dt_orswot:new(),
        metadata = snarl_map:new()
       }.

%%-spec load({integer(), atom()}, any_role()) -> role().

load(_, #?ROLE{} = Role) ->
    Role;

load(TID,
     #group_0_1_1{
        uuid = UUID,
        name = Name,
        permissions = Permissions,
        metadata = Metadata
       }) ->
    Rl = #role_0_1_0{
           uuid = UUID,
           name = Name,
           permissions = Permissions,
           metadata = Metadata
          },
    load(TID, update_permissions(TID, Rl));

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

load(IDT, RoleSB) ->
    Size = ?ENV(role_bucket_size, 50),
    Role = statebox:value(RoleSB),
    {ok, Name} = jsxd:get([<<"name">>], Role),
    {ok, UUID} = jsxd:get([<<"uuid">>], Role),
    ID0 = {{0,0,0}, load},
    Permissions0 = jsxd:get([<<"permissions">>], [], Role),
    Metadata = jsxd:get([<<"metadata">>], [], Role),
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

to_json(#?ROLE{
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

-spec merge(role(), role()) -> role().

merge(#?ROLE{
          uuid = UUID1,
          name = Name1,
          permissions = Permissions1,
          metadata = Metadata1
         },
      #?ROLE{
          uuid = UUID2,
          name = Name2,
          permissions = Permissions2,
          metadata = Metadata2
         }) ->
    #?ROLE{
        uuid = riak_dt_lwwreg:merge(UUID1, UUID2),
        name = riak_dt_lwwreg:merge(Name1, Name2),
        permissions = riak_dt_orswot:merge(Permissions1, Permissions2),
        metadata = snarl_map:merge(Metadata1, Metadata2)
       }.

name(Role) ->
    riak_dt_lwwreg:value(Role#?ROLE.name).

name({T, _ID}, Name, Role) ->
    {ok, V} = riak_dt_lwwreg:update({assign, Name, T}, none, Role#?ROLE.name),
    Role#?ROLE{name = V}.

uuid(Role) ->
    riak_dt_lwwreg:value(Role#?ROLE.uuid).

-spec uuid(Actor::term(), UUID::binary(), Role) ->
                  Role.
uuid({T, _ID}, UUID, Role = #?ROLE{}) ->
    {ok, V} = riak_dt_lwwreg:update({assign, UUID, T}, none, Role#?ROLE.uuid),
    Role#?ROLE{uuid = V}.

permissions(Role) ->
    riak_dt_orswot:value(Role#?ROLE.permissions).

grant({_T, ID}, Permission, Role = #?ROLE{}) ->
    {ok, V} = riak_dt_orswot:update({add, Permission},
                                    ID, Role#?ROLE.permissions),
    Role#?ROLE{permissions = V}.


revoke({_T, ID}, Permission, Role) ->
    {ok, V} =  riak_dt_orswot:update({remove, Permission},
                                     ID, Role#?ROLE.permissions),
    Role#?ROLE{permissions = V}.

revoke_prefix({_T, ID}, Prefix, Role) ->
    P0 = Role#?ROLE.permissions,
    Ps = permissions(Role),
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
    Role#?ROLE{
            permissions = P1
           }.

metadata(Role) ->
    Role#?ROLE.metadata.

set_metadata({T, ID}, P, Value, Role) when is_binary(P) ->
    set_metadata({T, ID}, snarl_map:split_path(P), Value, Role);

set_metadata({_T, ID}, Attribute, delete, Role) ->
    {ok, M1} = snarl_map:remove(Attribute, ID, Role#?ROLE.metadata),
    Role#?ROLE{metadata = M1};

set_metadata({T, ID}, Attribute, Value, Role) ->
    {ok, M1} = snarl_map:set(Attribute, Value, ID, T, Role#?ROLE.metadata),
    Role#?ROLE{metadata = M1}.

update_permissions(TID, Rl) ->
    Permissions = permissions(Rl),
    lists:foldl(fun ([<<"groups">> | R] = P, Acc) ->
                        Acc0 = revoke(TID, P, Acc),
                        grant(TID, [<<"roles">> | R], Acc0);
                    ([F, <<"groups">> | R] = P, Acc) ->
                        Acc0 = revoke(TID, P, Acc),
                        grant(TID, [F, <<"roles">> | R], Acc0);
                    ([F, S, <<"groups">> | R] = P, Acc) ->
                        Acc0 = revoke(TID, P, Acc),
                        grant(TID, [F, S, <<"roles">> | R], Acc0);
                    ([F, S, T, <<"groups">> | R] = P, Acc) ->
                        Acc0 = revoke(TID, P, Acc),
                        grant(TID, [F, S, T, <<"roles">> | R], Acc0);
                    (_, Acc) ->
                        Acc
                end, Rl, Permissions).

-ifdef(TEST).
mkid() ->
    {ecrdt:timestamp_us(), test}.

update_perms_test() ->
    %% Prepare permissions and expected update results
    P1 = [<<"groups">>, <<"g1">>, <<"get">>],
    P1u = [<<"roles">>, <<"g1">>, <<"get">>],
    P2 = [<<"cloud">>, <<"groups">>, <<"list">>],
    P2u = [<<"cloud">>, <<"roles">>, <<"list">>],
    P3 = [<<"vms">>, <<"v1">>, <<"get">>],

    %% Create role and grant permissions
    R = new(mkid()),
    R1 = grant(mkid(), P1, R),
    R2 = grant(mkid(), P2, R1),
    R3 = grant(mkid(), P3, R2),
    %% Check if the result is as expected
    Ps1 = lists:sort([P1, P2, P3]),
    RPs1 = permissions(R3),
    ?assertEqual(Ps1, RPs1),

    %% Update and check for changes.
    R4 = update_permissions(mkid(), R3),
    Ps1u = lists:sort([P1u, P2u, P3]),
    RPs1u = permissions(R4),
    ?assertEqual(Ps1u, RPs1u).

to_json_test() ->
    Role = new(mkid()),
    RoleJ = [{<<"metadata">>,[]},
             {<<"name">>,<<>>},
             {<<"permissions">>,[]},
             {<<"uuid">>,<<>>}],
    ?assertEqual(RoleJ, to_json(Role)).

name_test() ->
    Name0 = <<"Test0">>,
    Role0 = new(mkid()),
    Role1 = name(mkid(), Name0, Role0),
    Name1 = <<"Test1">>,
    Role2 = name(mkid(), Name1, Role1),
    ?assertEqual(Name0, name(Role1)),
    ?assertEqual(Name1, name(Role2)).

permissions_test() ->
    P0 = [<<"P0">>],
    P1 = [<<"P1">>],
    Role0 = new(mkid()),
    Role1 = grant(mkid(), P0, Role0),
    Role2 = grant(mkid(), P1, Role1),
    Role3 = grant(mkid(), P0, Role2),
    Role4 = revoke(mkid(), P0, Role3),
    Role5 = revoke(mkid(), P1, Role3),
    ?assertEqual([P0], permissions(Role1)),
    ?assertEqual([P0, P1], permissions(Role2)),
    ?assertEqual([P0, P1], permissions(Role3)),
    ?assertEqual([P1], permissions(Role4)),
    ?assertEqual([P0], permissions(Role5)).

-endif.
