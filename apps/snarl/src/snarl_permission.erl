-module(snarl_permission).

-export([grant/2, revoke/2, revoke_prefix/2]).

grant(Permission, Permissions) ->
    ordsets:add_element(Permission, Permissions).

revoke(Permission, Permissions) ->
    ordsets:del_element(Permission, Permissions).

revoke_prefix(Prefix, Permissions) ->
    [P || P <- Permissions, not lists:prefix(Prefix, P)].
