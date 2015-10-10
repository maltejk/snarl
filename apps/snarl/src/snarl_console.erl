%% @doc Interface for snarl-admin commands.
-module(snarl_console).

-include("snarl.hrl").

-export([
         db_keys/1,
         db_get/1,
         db_delete/1,
         get_ring/1,
         db_update/1,
         config/1,
         aae_status/1,
         init_user/1
        ]).
-export([add_role/1,
         delete_role/1,
         join_role/1,
         leave_role/1,
         grant_role/1,
         list_role/1,
         revoke_role/1]).

-export([add_user/1,
         delete_user/1,
         list_user/1,
         grant_user/1,
         revoke_user/1,
         passwd/1]).

-export([scope_list/1,
         scope_del/1,
         scope_grant/1,
         scope_revoke/1,
         scope_add/1,
         scope_toggle/1]).

-ignore_xref([
              db_keys/1,
              db_get/1,
              db_delete/1,
              get_ring/1,
              db_update/1,
              delete_user/1,
              delete_role/1,
              aae_status/1,
              list_user/1,
              list_role/1,
              add_user/1,
              add_role/1,
              join_role/1,
              leave_role/1,
              grant_role/1,
              grant_user/1,
              revoke_user/1,
              revoke_role/1,
              passwd/1,
              config/1,
              scope_list/1,
              scope_del/1,
              scope_add/1,
              scope_grant/1,
              scope_revoke/1,
              scope_toggle/1,
              init_user/1
             ]).

db_update([]) ->
    db_update(["default"]);

db_update([Realm]) ->
    [db_update([Realm, E]) || E <- ["users", "roles", "orgs"]],
    ok;

db_update([Realm, "users"]) ->
    io:format("Updating users...~n"),
    do_update(Realm, snarl_user, ft_user);

db_update([Realm, "roles"]) ->
    io:format("Updating roles...~n"),
    do_update(Realm, snarl_role, ft_role);

db_update([Realm, "orgs"]) ->
    io:format("Updating orgs...~n"),
    do_update(Realm, snarl_org, ft_org).

get_ring([]) ->
    {ok, RingData} = riak_core_ring_manager:get_my_ring(),
    {_S, CHash} = riak_core_ring:chash(RingData),
    io:format("Hash                "
              "                    "
              "          "
              " Node~n"),
    io:format("--------------------"
              "--------------------"
              "----------"
              " ------------------------------~n", []),
    lists:map(fun({K, H}) ->
                      io:format("~50b ~-30s~n", [K, H])
              end, CHash),
    ok.

is_prefix(Prefix, K) ->
    binary:longest_common_prefix([Prefix, K]) =:= byte_size(Prefix).

db_delete([CHashS, CatS, KeyS]) ->
    Cat = list_to_binary(CatS),
    Key = list_to_binary(KeyS),
    CHash = list_to_integer(CHashS),
    {ok, RingData} = riak_core_ring_manager:get_my_ring(),
    {_S, CHashs} = riak_core_ring:chash(RingData),
    case lists:keyfind(CHash, 1, CHashs) of
        false ->
            io:format("C-Hash ~b does not exist.~n", [CHash]),
            error;
        _ ->
            CHashA = list_to_atom(CHashS),
            fifo_db:delete(CHashA, Cat, Key)
    end.
db_get([CHashS, CatS, KeyS]) ->
    Cat = list_to_binary(CatS),
    Key = list_to_binary(KeyS),
    CHash = list_to_integer(CHashS),
    {ok, RingData} = riak_core_ring_manager:get_my_ring(),
    {_S, CHashs} = riak_core_ring:chash(RingData),
    case lists:keyfind(CHash, 1, CHashs) of
        false ->
            io:format("C-Hash ~b does not exist.~n", [CHash]),
            error;
        _ ->
            CHashA = list_to_atom(CHashS),
            case fifo_db:get(CHashA, Cat, Key) of
                {ok, E} ->
                    io:format("~p~n", [E]);
                _ ->
                    io:format("Not found.~n", []),
                    error
            end
    end.

db_keys([]) ->
    db_keys(["-p", ""]);

db_keys(["-p", PrefixS]) ->
    Prefix = list_to_binary(PrefixS),
    {ok, RingData} = riak_core_ring_manager:get_my_ring(),
    {_S, CHashs} = riak_core_ring:chash(RingData),
    lists:map(fun({Hash, H}) ->
                      io:format("~n~50b ~-15s~n", [Hash, H]),
                      io:format("--------------------"
                                "--------------------"
                                "--------------------"
                                "------~n", []),
                      CHashA = list_to_atom(integer_to_list(Hash)),
                      Keys = fifo_db:list_keys(CHashA, <<>>),
                      [io:format("~s~n", [K])
                       || K <- Keys,
                          is_prefix(Prefix, K) =:= true]
              end, CHashs),
    ok;

db_keys([CHashS]) ->
    db_keys([CHashS, ""]);

db_keys([CHashS, PrefixS]) ->
    Prefix = list_to_binary(PrefixS),
    CHash = list_to_integer(CHashS),
    {ok, RingData} = riak_core_ring_manager:get_my_ring(),
    {_S, CHashs} = riak_core_ring:chash(RingData),
    case lists:keyfind(CHash, 1, CHashs) of
        false ->
            io:format("C-Hash ~b does not exist.", [CHash]),
            error;
        _ ->
            CHashA = list_to_atom(CHashS),
            Keys = fifo_db:list_keys(CHashA, Prefix),
            [io:format("~s~n", [K]) || K <- Keys],
            ok
    end.

list_user([RealmS]) ->
    Realm = list_to_binary(RealmS),
    {ok, Users} = snarl_user:list(Realm),
    io:format("UUID                                 Name~n"),
    io:format("------------------------------------ ------------------------------~n", []),
    lists:map(fun(UUID) ->
                      {ok, User} = snarl_user:get(Realm, UUID),
                      io:format("~36s ~-30s~n",
                                [UUID, ft_user:name(User)])
              end, Users),
    ok.
list_role([RealmS]) ->
    Realm = list_to_binary(RealmS),
    {ok, Users} = snarl_role:list(Realm),
    io:format("UUID                                 Name~n"),
    io:format("------------------------------------ ------------------------------~n", []),
    lists:map(fun(UUID) ->
                      {ok, User} = snarl_role:get(Realm, UUID),
                      io:format("~36s ~-30s~n",
                                [UUID, ft_role:name(User)])
              end, Users),
    ok.

delete_user([RealmS, User]) ->
    Realm = list_to_binary(RealmS),
    snarl_user:delete(Realm, list_to_binary(User)),
    ok.

delete_role([RealmS, User]) ->
    Realm = list_to_binary(RealmS),
    snarl_user:delete(Realm, list_to_binary(User)),
    ok.

init_user([RealmS, OrgS, RoleS, UserS, PassS]) ->
    Realm = list_to_binary(RealmS),
    Org = list_to_binary(OrgS),
    Role = list_to_binary(RoleS),
    User = list_to_binary(UserS),
    Pass = list_to_binary(PassS),
    {ok, UserUUID} = snarl_user:add(Realm, User),
    io:format("Created user '~s' with id ~s.~n", [User, UserUUID]),
    ok = snarl_user:grant(Realm, UserUUID, [<<"...">>]),
    io:format("Granted full permissions to ~s.~n", [User]),
    ok = snarl_user:passwd(Realm, UserUUID, Pass),
    io:format("Set password for to ~s.~n", [User]),
    {ok, OrgUUID} = snarl_org:add(Realm, Org),
    io:format("Created org '~s' with id ~s.~n", [Org, OrgUUID]),
    ok = snarl_user:grant(Realm, UserUUID, [<<"orgs">>, OrgUUID, <<"_">>]),
    io:format("Granted permission on org ~s to ~s.~n", [Org, User]),
    ok = snarl_user:join_org(Realm, UserUUID, OrgUUID),
    io:format("Joined ~s to ~s.~n", [User, Org]),
    ok = snarl_user:select_org(Realm, UserUUID, OrgUUID),
    io:format("Selected ~s as active org for ~s.~n", [Org, User]),
    {ok, RoleUUID} = snarl_role:add(Realm, Role),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"cloud">>, <<"cloud">>, <<"status">>]),

    ok = snarl_role:grant(Realm, RoleUUID, [<<"cloud">>, <<"datasets">>, <<"list">>]),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"cloud">>, <<"networks">>, <<"list">>]),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"cloud">>, <<"ipranges">>, <<"list">>]),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"cloud">>, <<"packages">>, <<"list">>]),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"cloud">>, <<"roles">>, <<"list">>]),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"cloud">>, <<"orgs">>, <<"list">>]),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"cloud">>, <<"users">>, <<"list">>]),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"cloud">>, <<"vms">>, <<"list">>]),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"cloud">>, <<"hypervisors">>, <<"list">>]),

    ok = snarl_role:grant(Realm, RoleUUID, [<<"cloud">>, <<"vms">>, <<"create">>]),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"hypervisors">>, <<"_">>, <<"create">>]),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"datasets">>, <<"_">>, <<"create">>]),
    ok = snarl_role:grant(Realm, RoleUUID, [<<"roles">>, RoleUUID, <<"get">>]),
    io:format("Added default role ~s (~s).~n", [Role, RoleUUID]),
    snarl_opt:set([users, Realm, initial_role], RoleUUID),
    snarl_oauth:add_scope(Realm, <<"*">>, <<"Everything">>),
    snarl_oauth:add_permission(Realm, <<"*">>, [<<"...">>]),
    snarl_oauth:default(Realm, <<"*">>),
    io:format("Added 'Everything' scope and set it default.~n", []),
    ok.

add_user([RealmS, User]) ->
    Realm = list_to_binary(RealmS),
    case snarl_user:add(Realm, list_to_binary(User)) of
        {ok, UUID} ->
            io:format("User '~s' added with id '~s'.~n", [User, UUID]),
            ok;
        duplicate ->
            io:format("User '~s' already exists.~n", [User]),
            error
    end.

add_role([RealmS, Role]) ->
    Realm = list_to_binary(RealmS),
    case snarl_role:add(Realm, list_to_binary(Role)) of
        {ok, UUID} ->
            io:format("Role '~s' added with id '~s'.~n", [Role, UUID]),
            ok;
        duplicate ->
            io:format("Role '~s' already exists.~n", [Role]),
            error
    end.

join_role([RealmS, User, Role]) ->
    Realm = list_to_binary(RealmS),
    case snarl_user:lookup(Realm, list_to_binary(User)) of
        {ok, UserObj} ->
            case snarl_role:lookup(Realm, list_to_binary(Role)) of
                {ok, RoleObj} ->
                    ok = snarl_user:join(Realm,
                                         ft_user:uuid(UserObj),
                                         ft_role:uuid(RoleObj)),
                    io:format("User '~s' added to role '~s'.~n", [User, Role]),
                    ok;
                _ ->
                    io:format("Role does not exist.~n"),
                    error
            end;
        _ ->
            io:format("User does not exist.~n"),
            error
    end.

leave_role([RealmS, User, Role]) ->
    Realm = list_to_binary(RealmS),
    case snarl_user:lookup(Realm, list_to_binary(User)) of
        {ok, UserObj} ->
            case snarl_role:lookup(Realm, list_to_binary(Role)) of
                {ok, RoleObj} ->
                    ok = snarl_user:leave(Realm,
                                          ft_user:uuid(UserObj),
                                          ft_role:uuid(RoleObj)),
                    io:format("User '~s' removed from role '~s'.~n", [User, Role]),
                    ok;
                _ ->
                    io:format("Role does not exist.~n"),
                    error
            end;
        _ ->
            io:format("User does not exist.~n"),
            error
    end.

passwd([RealmS, User, Pass]) ->
    Realm = list_to_binary(RealmS),
    case snarl_user:lookup(Realm, list_to_binary(User)) of
        {ok, UserObj} ->
            case snarl_user:passwd(Realm,
                                   ft_user:uuid(UserObj),
                                   list_to_binary(Pass)) of
                ok ->
                    io:format("Password successfully changed for user '~s'.~n", [User]),
                    ok;
                not_found ->
                    io:format("User '~s' not found.~n", [User]),
                    error
            end;
        _ ->
            io:format("User does not exist.~n"),
            error
    end.

grant_role([RealmS, Role | P]) ->
    Realm = list_to_binary(RealmS),
    case snarl_role:lookup(Realm, list_to_binary(Role)) of
        {ok, RoleObj} ->
            case snarl_role:grant(Realm, ft_role:uuid(RoleObj),
                                  build_permission(P)) of
                ok ->
                    io:format("Granted.~n", []),
                    ok;
                _ ->
                    io:format("Failed.~n", []),
                    error
            end;
        not_found ->
            io:format("Role '~s' not found.~n", [Role]),
            error
    end.

grant_user([RealmS, User | P ]) ->
    Realm = list_to_binary(RealmS),
    case snarl_user:lookup(Realm, list_to_binary(User)) of
        {ok, UserObj} ->
            case snarl_user:grant(Realm,
                                  ft_user:uuid(UserObj),
                                  build_permission(P)) of
                ok ->
                    io:format("Granted.~n", []),
                    ok;
                not_found ->
                    io:format("User '~s' not found.~n", [User]),
                    error
            end;
        _ ->
            io:format("User does not exist.~n"),
            error
    end.

revoke_user([RealmS, User | P ]) ->
    Realm = list_to_binary(RealmS),
    case snarl_user:lookup(Realm, list_to_binary(User)) of
        {ok, UserObj} ->
            case snarl_user:revoke(Realm,
                                   ft_user:uuid(UserObj),
                                   build_permission(P)) of
                ok ->
                    io:format("Granted.~n", []),
                    ok;
                not_found ->
                    io:format("User '~s' not found.~n", [User]),
                    error
            end;
        _ ->
            io:format("User does not exist.~n"),
            error
    end.

revoke_role([RealmS, Role | P]) ->
    Realm = list_to_binary(RealmS),
    case snarl_role:lookup(Realm, list_to_binary(Role)) of
        {ok, RoleObj} ->
            case snarl_role:revoke(Realm,
                                   ft_role:uuid(RoleObj),
                                   build_permission(P)) of
                ok ->
                    io:format("Revoked.~n", []),
                    ok;
                _ ->
                    io:format("Failed.~n", []),
                    error
            end;
        not_found ->
            io:format("Role '~s' not found.~n", [Role]),
            error
    end.

aae_status(["accounting"]) ->
    fifo_console:aae_status({snarl_accounting, "Accounting"});

aae_status([]) ->
    Services = [{snarl_user, "User"}, {snarl_role, "Role"},
                {snarl_org, "Org"}, {snarl_accounting, "Accounting"}],
    [fifo_console:aae_status(E) || E <- Services].


config(["show", Realm]) ->
    io:format("Defaults for ~s~n  User Section~n", [Realm]),
    fifo_console:print_config(users, Realm),
    io:format("~n  User Section~n"),
    fifo_console:print_config(clients, Realm),
    ok;

config(["show"]) ->
    io:format("Defaults~n  Yubikey~n"),
    fifo_console:print_config(yubico, api),
    ok;

config(["set", Ks, V]) ->
    Ks1 = [binary_to_list(K) || K <- re:split(Ks, "\\.")],
    config(["set" | Ks1] ++ [V]);

config(["set" | R]) ->
    [K1, K2, K3, V] = R,
    Ks = [K1, K2, K3],
    case snarl_opt:set(Ks, V) of
        {invalid, key, K} ->
            io:format("Invalid key: ~p~n", [K]),
            error;
        {invalid, type, T} ->
            io:format("Invalid type: ~p~n", [T]),
            error;
        _ ->
            io:format("Setting changed~n", []),
            ok
    end;

config(["unset", Ks]) ->
    Ks1 = [binary_to_list(K) || K <- re:split(Ks, "\\.")],
    config(["unset" | Ks1]);

config(["unset" | Ks]) ->
    snarl_opt:unset(Ks),
    io:format("Setting changed~n", []),
    ok.

scope_list([RealmS]) ->
    Realm = list_to_binary(RealmS),
    io:format("Registered scopes:~n"),
    io:format("~-25s ~-30s ~-7s ~s~n",
              ["Scope", "Description", "Default", "Permissions"]),
    [
     io:format("~-25s ~-30s ~-7s ~s~n", [Scope, Desc, Dflt, fmt_perms(Perms)])
     || #{scope := Scope, desc := Desc,
          default := Dflt, permissions := Perms} <- snarl_oauth:scope(Realm)
    ],
    ok.

scope_add([RealmS, ScopeS | DescS]) ->
    Realm = list_to_binary(RealmS),
    Scope = list_to_binary(ScopeS),
    Desc  = list_to_binary(string:join(DescS, " ")),

    case find_scope(Realm, Scope) of
        [] ->
            io:format("Add ~s ~s '~s'.~n", [Realm, Scope, Desc]),
            snarl_oauth:add_scope(Realm, Scope, Desc),
            ok;
        _ ->
            io:format("Scope ~s already defined in realm ~s.~n",
                      [Scope, Realm]),
            error
    end.

scope_del([RealmS, ScopeS]) ->
    Realm = list_to_binary(RealmS),
    Scope = list_to_binary(ScopeS),
    case find_scope(Realm, Scope) of
        [] ->
            io:format("Scope ~s nor defined in realm ~s.~n", [Scope, Realm]),
            ok;
        _ ->
            io:format("Deleting sope ~s in realm ~s.~n", [Realm, Scope]),
            snarl_oauth:delete_scope(Realm, Scope),
            ok
    end.

scope_grant([RealmS, ScopeS | PermS]) ->
    Realm = list_to_binary(RealmS),
    Scope = list_to_binary(ScopeS),
    Perm = [list_to_binary(P) || P <- PermS],
    case snarl_oauth:add_permission(Realm, Scope, Perm) of
        {error, not_found} ->
            io:format("Scope ~s nor defined in realm ~s.~n", [Scope, Realm]),
            ok;
        _ ->
            io:format("Permission added to sope ~s in realm ~s.~n",
                      [Realm, Scope]),
            ok
    end.

scope_revoke([RealmS, ScopeS | PermS]) ->
    Realm = list_to_binary(RealmS),
    Scope = list_to_binary(ScopeS),
    Perm = [list_to_binary(P) || P <- PermS],
    case snarl_oauth:remove_permission(Realm, Scope, Perm) of
        {error, not_found} ->
            io:format("Scope ~s nor defined in realm ~s.~n", [Scope, Realm]),
            ok;
        _ ->
            io:format("Permission added to sope ~s in realm ~s.~n",
                      [Realm, Scope]),
            ok
    end.

scope_toggle([RealmS, ScopeS]) ->
    Realm = list_to_binary(RealmS),
    Scope = list_to_binary(ScopeS),
    case snarl_oauth:default(Realm, Scope) of
        {error, not_found} ->
            io:format("Scope ~s nor defined in realm ~s.~n", [Scope, Realm]),
            ok;
        _ ->
            io:format("Toggled default for scope sope ~s in realm ~s.~n",
                      [Realm, Scope]),
            ok
    end.

%%%===================================================================
%%% Private
%%%===================================================================

build_permission(P) ->
    lists:map(fun list_to_binary/1, P).

do_update(RealmS, MainMod, StateMod) ->
    Realm = list_to_binary(RealmS),
    {ok, US} = MainMod:list_(undefined),
    io:format("  Entries found: ~p~n", [length(US)]),
    io:format("  Grabbing UUIDs"),
    ID = snarl_vnode:mkid(),
    US1 = [begin
               io:format("."),
               U1 = ft_obj:update(U),
               {StateMod:uuid(StateMod:load(ID, ft_obj:val(U1))), U1}
           end|| U <- US],
    io:format(" done.~n"),

    io:format("  Wipeing old entries"),
    [begin
         io:format("."),
         MainMod:wipe(undefined, UUID)
     end || {UUID, _} <- US1],
    io:format(" done.~n"),

    io:format("  Restoring entries"),
    [begin
         io:format("."),
         MainMod:sync_repair(Realm, UUID, O)
     end || {UUID, O} <- US1],
    io:format(" done.~n"),
    io:format("Update complete.~n"),
    ok.

fmt_perms(Perms) ->
    L1 = [fmt_perm(P) || P <- Perms],
    string:join(L1, ", ").

fmt_perm(Perm) ->
    L1 = [binary_to_list(P) || P <- Perm],
    string:join(L1, "->").


find_scope(Realm, Scope) ->
    [S || S = #{scope := Name} <- snarl_oauth:scope(Realm), Name =:= Scope].
