-module(snarl_oauth).

-export([scope/1, scope/2, add_scope/3, delete_scope/2, add_permission/3,
         remove_permission/3]).

-ignore_xref([scope/1, scope/2, add_scope/3, delete_scope/2, add_permission/3,
         remove_permission/3]).

scope(Realm) ->
    case riak_core_metadata:get({<<"oauth">>, Realm}, <<"scope">>) of
        undefined ->
            [];
        V ->
            V
    end.

scope(Realm, Subscope) ->
    Set = oauth2_priv_set:new(Subscope),
    AS = scope(Realm),
    [{S, Desc, Perms}
     || {S, Desc, Perms} <- AS,
        oauth2_priv_set:is_member(S, Set)].

add_scope(Realm, Scope, Description) ->
    OldScopes = scope(Realm),
    OldScopes1 = [S || S = {Name, _Desc, _Perms} <- OldScopes, Name =/= Scope],
    OldScopes2 = [{Scope, Description, []} | OldScopes1],
    riak_core_metadata:put({<<"oauth">>, Realm}, <<"scope">>, OldScopes2).

delete_scope(Realm, Scope) ->
    OldScopes = scope(Realm),
    OldScopes1 = [S || S = {Name, _Desc, _Perms} <- OldScopes, Name =/= Scope],
    riak_core_metadata:put({<<"oauth">>, Realm}, <<"scope">>, OldScopes1).

add_permission(Realm, Scope, Permission) ->
    Scopes = scope(Realm),
    case [S || S = {Name, _, _} <- Scopes, Name =:= Scope] of
        [] ->
            {error, not_found};
        [{Scope, Description, Permissions}] ->
            Scopes1 = [S || S = {Name, _, _} <- Scopes, Name =/= Scope],
            Permissions2 = [Permission|Permissions],
            Scopes2 = [{Scope, Description, Permissions2} | Scopes1],
            riak_core_metadata:put({<<"oauth">>, Realm}, <<"scope">>, Scopes2)
    end.

remove_permission(Realm, Scope, Permission) ->
    Scopes = scope(Realm),
    case [S || S = {Name, _, _} <- Scopes, Name =:= Scope] of
        [] ->
            {error, not_found};
        [{Scope, Description, Permissions}] ->
            Scopes1 = [S || S = {Name, _, _} <- Scopes, Name =/= Scope],
            Permissions2 = [P || P <- Permissions, P =/= Permission],
            Scopes2 = [{Scope, Description, Permissions2} | Scopes1],
            riak_core_metadata:put({<<"oauth">>, Realm}, <<"scope">>, Scopes2)
    end.
