-module(snarl_opt).

-export([get/3, set/2, unset/1, update/0]).
-ignore_xref([update/0]).

get(Prefix, SubPrefix, Key) ->
    fifo_opt:get(Prefix, SubPrefix, Key).

set(Ks, Val) ->
    fifo_opt:set(opts(), Ks, Val).

unset(Ks) ->
    fifo_opt:unset(opts(), Ks).

update() ->
    Opts =
        [
         {defaults, users, initial_role},
         {defaults, users, initial_org},
         {defaults, clients, initial_role},
         {yubico, api, client_id},
         {yubico, api, secret_key}
        ],
    [update(A, B, C) || {A, B, C} <- Opts].

update(A, B, C) ->
    case riak_core_metadata:get({A, B}, C) of
        undefined ->
            ok;
        V ->
            riak_core_metadata:delete({A, B}, C),
            set([A, B, C], V)
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================


opts() ->
    [{"users",
      [{'_',
        [{"initial_role", binary}, {"initial_org", binary}]}]},
     {"clients",
      [{'_',
        [{"initial_role", binary}]}]},
     {"yubico",
      [{"api", [{"client_id", integer}, {"secret_key", binary}]}]}].
