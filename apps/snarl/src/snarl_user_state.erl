%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(snarl_user_state).

-include("snarl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         new/0,
         load/1,
         uuid/1, uuid/3,
         name/1, name/3,
         password/1, password/3,
         permissions/1, grant/3, revoke/3, revoke_prefix/3,
         groups/1, join/3, leave/3,
         join_org/3, leave_org/3, select_org/3, orgs/1, active_org/1,
         add_key/4, revoke_key/3, keys/1,
         metadata/1, set_metadata/3,
         merge/2,
         to_json/1,
         expire/2,
         gc/3,
         gcable/1
        ]).

-ignore_xref([
              new/0,
              load/1,
              uuid/1, uuid/3,
              name/1, name/3,
              password/1, password/3,
              permissions/1, grant/3, revoke/3, revoke_prefix/3,
              groups/1, join/3, leave/3,
              add_key/4, revoke_key/3, keys/1,
              metadata/1, set_metadata/3,
              join_org/3, leave_org/3, select_org/3, orgs/1, active_org/1,
              merge/2,
              to_json/1,
              expire/2,
              gc/3,
              gcable/1
             ]).

join_org(ID, Org, User) ->
    User#?USER{
            orgs =
                vorsetg:add(ID, Org, User#?USER.orgs)
           }.

leave_org(ID, Org, User) ->
    User#?USER{
            orgs =
                vorsetg:remove(ID, Org, User#?USER.orgs)
           }.

select_org(_, Org, User) ->
    User#?USER{
            active_org = vlwwregister:assign(Org,
                                             User#?USER.active_org)
           }.

orgs(User) ->
    vorsetg:value(User#?USER.orgs).

active_org(User) ->
    vlwwregister:value(User#?USER.active_org).

load(#?USER{} = User) ->
    User;

load(#user_0_1_0{
        uuid = UUID,
        name = Name,
        password = Passwd,
        permissions = Permissions,
        groups = Groups,
        metadata = Metadata
       }) ->
    Size = ?ENV(user_bucket_size, 50),
    load(#user_0_1_1{
            uuid = UUID,
            name = Name,
            password = Passwd,
            permissions = Permissions,
            groups = Groups,
            ssh_keys = vorsetg:new(Size),
            metadata = Metadata});

load(#user_0_1_1{
        uuid = UUID,
        name = Name,
        password = Passwd,
        permissions = Permissions,
        groups = Groups,
        ssh_keys = Keys,
        metadata = Metadata
       }) ->
    Size = ?ENV(user_bucket_size, 50),
    load(#user_0_1_2{
            uuid = UUID,
            name = Name,
            password = Passwd,
            permissions = Permissions,
            active_org = vlwwregister:new(<<"">>),
            groups = Groups,
            ssh_keys = Keys,
            orgs = vorsetg:new(Size),
            metadata = Metadata});

load(UserSB) ->
    Size = ?ENV(user_bucket_size, 50),
    User = statebox:value(UserSB),
    {ok, Name} = jsxd:get([<<"name">>], User),
    {ok, UUID} = jsxd:get([<<"uuid">>], User),
    Password = jsxd:get([<<"password">>], <<>>, User),
    ID0 = {{0,0,0}, load},
    Groups0 = jsxd:get([<<"groups">>], [], User),
    Permissions0 = jsxd:get([<<"permissions">>], [], User),
    Metadata = jsxd:get([<<"metadata">>], [], User),
    Groups = lists:foldl(
               fun (G, Acc) ->
                       vorsetg:add(ID0, G, Acc)
               end, vorsetg:new(Size), Groups0),
    Permissions = lists:foldl(
                    fun (G, Acc) ->
                            vorsetg:add(ID0, G, Acc)
                    end, vorsetg:new(Size), Permissions0),
    load(#user_0_1_0{
            uuid = vlwwregister:new(UUID),
            name = vlwwregister:new(Name),
            password = vlwwregister:new(Password),
            groups = Groups,
            permissions = Permissions,
            metadata = statebox:new(fun() -> Metadata end)}).

gcable(#?USER{
           permissions = Permissions,
           groups = Groups,
           ssh_keys = Keys,
           orgs = Orgs
          }) ->
    {vorsetg:gcable(Permissions), vorsetg:gcable(Groups),
     vorsetg:gcable(Keys), vorsetg:gcable(Orgs)}.

gc(_ID,
   {Ps, Gs, Ks, Os},
   #?USER{
       permissions = Permissions,
       groups = Groups,
       ssh_keys = Keys,
       orgs = Orgs
      } = User) ->
    Ps1 = lists:foldl(fun vorsetg:gc/2, Permissions, Ps),
    Gs1 = lists:foldl(fun vorsetg:gc/2, Groups, Gs),
    Ks1 = lists:foldl(fun vorsetg:gc/2, Keys, Ks),
    Os1 = lists:foldl(fun vorsetg:gc/2, Orgs, Os),
    User#?USER{
            permissions = Ps1,
            groups = Gs1,
            ssh_keys = Ks1,
            orgs = Os1
           }.

new() ->
    Size = ?ENV(user_bucket_size, 50),
    #?USER{
        uuid = vlwwregister:new(<<>>),
        name = vlwwregister:new(<<>>),
        password = vlwwregister:new(<<>>),
        active_org = vlwwregister:new(<<>>),
        groups = vorsetg:new(Size),
        permissions = vorsetg:new(Size),
        ssh_keys = vorsetg:new(Size),
        orgs = vorsetg:new(Size),
        metadata = statebox:new(fun jsxd:new/0)
       }.

to_json(#?USER{
            uuid = UUID,
            name = Name,
            groups = Groups,
            ssh_keys = Keys,
            permissions = Permissions,
            active_org = Org,
            orgs = Orgs,
            metadata = Metadata
           }) ->
    jsxd:from_list(
      [
       {<<"uuid">>, vlwwregister:value(UUID)},
       {<<"name">>, vlwwregister:value(Name)},
       {<<"groups">>, vorsetg:value(Groups)},
       {<<"permissions">>, vorsetg:value(Permissions)},
       {<<"keys">>, vorsetg:value(Keys)},
       {<<"org">>, vlwwregister:value(Org)},
       {<<"orgs">>, vorsetg:value(Orgs)},
       {<<"metadata">>, statebox:value(Metadata)}
      ]).

merge(#?USER{
          uuid = UUID1,
          name = Name1,
          password = Password1,
          groups = Groups1,
          permissions = Permissions1,
          ssh_keys = Keys1,
          active_org = Org1,
          orgs = Orgs1,
          metadata = Metadata1
         },
      #?USER{
          uuid = UUID2,
          name = Name2,
          password = Password2,
          groups = Groups2,
          permissions = Permissions2,
          ssh_keys = Keys2,
          active_org = Org2,
          orgs = Orgs2,
          metadata = Metadata2
         }) ->
    #?USER{
        uuid = vlwwregister:merge(UUID1, UUID2),
        name = vlwwregister:merge(Name1, Name2),
        password = vlwwregister:merge(Password1, Password2),
        active_org = vlwwregister:merge(Org1, Org2),
        groups = vorsetg:merge(Groups1, Groups2),
        ssh_keys = vorsetg:merge(Keys1, Keys2),
        permissions = vorsetg:merge(Permissions1, Permissions2),
        orgs = vorsetg:merge(Orgs1, Orgs2),
        metadata = statebox:merge([Metadata1, Metadata2])
       }.

add_key(ID, KeyID, Key, User) ->
    User#?USER{
            ssh_keys =
                vorsetg:add(ID, {KeyID, Key}, User#?USER.ssh_keys)
           }.

revoke_key(ID, KeyID, User) ->
    case lists:keyfind(KeyID, 1, keys(User)) of
        false ->
            User;
        Tpl ->
            User#?USER{
                    ssh_keys =
                        vorsetg:remove(ID, Tpl, User#?USER.ssh_keys)
                   }
    end.

keys(User) ->
    vorsetg:value(User#?USER.ssh_keys).

name(User) ->
    vlwwregister:value(User#?USER.name).

name(_, Name, User) ->
    User#?USER{
            name = vlwwregister:assign(Name, User#?USER.name)
           }.

uuid(User) ->
    vlwwregister:value(User#?USER.uuid).

uuid(_, UUID, User) ->
    User#?USER{
            uuid = vlwwregister:assign(UUID, User#?USER.uuid)
           }.

password(User) ->
    vlwwregister:value(User#?USER.password).

password(_, Hash, User) ->
    User#?USER{
            password = vlwwregister:assign(Hash,
                                           User#?USER.password)
           }.

permissions(User) ->
    vorsetg:value(User#?USER.permissions).

grant(ID, Permission, User) ->
    User#?USER{
            permissions =
                vorsetg:add(ID, Permission, User#?USER.permissions)
           }.


revoke(ID, Permission, User) ->
    User#?USER{
            permissions =
                vorsetg:remove(ID, Permission, User#?USER.permissions)
           }.

revoke_prefix(ID, Prefix, User) ->
    P0 = User#?USER.permissions,
    Ps = permissions(User),
    P1 = lists:foldl(fun (P, PAcc) ->
                             case lists:prefix(Prefix, P) of
                                 true ->
                                     vorsetg:remove(ID, P, PAcc);
                                 _ ->
                                     PAcc
                             end
                     end, P0, Ps),
    User#?USER{
            permissions = P1
           }.

groups(User) ->
    vorsetg:value(User#?USER.groups).


join(ID, Group, User) ->
    User#?USER{
            groups =
                vorsetg:add(ID, Group, User#?USER.groups)
           }.

leave(ID, Group, User) ->
    User#?USER{
            groups =
                vorsetg:remove(ID, Group, User#?USER.groups)
           }.

metadata(User) ->
    User#?USER.metadata.

set_metadata(Attribute, delete, User) ->
    User#?USER{
            metadata =
                statebox:modify({fun jsxd:delete/2,
                                 [Attribute]}, User#?USER.metadata)
           };

set_metadata(Attribute, Value, User) ->
    User#?USER{
            metadata =
                statebox:modify({fun jsxd:set/3,
                                 [Attribute, Value]}, User#?USER.metadata)
           }.

expire(Timeout, User) ->
    User#?USER{
            metadata =
                statebox:expire(Timeout, User#?USER.metadata)
           }.

-ifdef(TEST).

mkid() ->
    {ecrdt:timestamp_us(), test}.

to_json_test() ->
    User = new(),
    UserJ = [{<<"groups">>,[]},
             {<<"keys">>,[]},
             {<<"metadata">>,[]},
             {<<"name">>,<<>>},
             {<<"org">>,<<>>},
             {<<"orgs">>,[]},
             {<<"permissions">>,[]},
             {<<"uuid">>,<<>>}],
    ?assertEqual(UserJ, to_json(User)).

name_test() ->
    Name0 = <<"Test0">>,
    User0 = new(),
    User1 = name(mkid(), Name0, User0),
    Name1 = <<"Test1">>,
    User2 = name(mkid(), Name1, User1),
    ?assertEqual(Name0, name(User1)),
    ?assertEqual(Name1, name(User2)).

password_test() ->
    Name = "Test",
    Password = "Test",
    User0 = new(),
    User1 = name(mkid(), Name, User0),
    User2 = password(mkid(), Password, User1),
    ?assertEqual(Password, password(User2)).

permissions_test() ->
    P0 = [<<"P0">>],
    P1 = [<<"P1">>],
    User0 = new(),
    User1 = grant(mkid(), P0, User0),
    User2 = grant(mkid(), P1, User1),
    User3 = grant(mkid(), P0, User2),
    User4 = revoke(mkid(), P0, User3),
    User5 = revoke(mkid(), P1, User3),
    ?assertEqual([P0], permissions(User1)),
    ?assertEqual([P0, P1], permissions(User2)),
    ?assertEqual([P0, P1], permissions(User3)),
    ?assertEqual([P1], permissions(User4)),
    ?assertEqual([P0], permissions(User5)).

groups_test() ->
    G0 = "G0",
    G1 = "G1",
    User0 = new(),
    User1 = join(mkid(), G0, User0),
    User2 = join(mkid(), G1, User1),
    User3 = join(mkid(), G0, User2),
    User4 = leave(mkid(), G0, User3),
    User5 = leave(mkid(), G1, User3),
    ?assertEqual([G0], groups(User1)),
    ?assertEqual([G0, G1], groups(User2)),
    ?assertEqual([G0, G1], groups(User3)),
    ?assertEqual([G1], groups(User4)),
    ?assertEqual([G0], groups(User5)).

-endif.
