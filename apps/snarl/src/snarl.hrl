%-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(PRINT(Var), 1 == 1 orelse io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

-define(ENV(K, D),
        (case application:get_env(snarl, K) of
             undefined ->
                 D;
             {ok, EnvValue} ->
                 EnvValue
         end)).

-define(N, ?ENV(n, 3)).
-define(R, ?ENV(r, 2)).
-define(W, ?ENV(w, 3)).
-define(NRW(System),
        (case application:get_key(System) of
             {ok, Res} ->
                 Res;
             undefined ->
                 {?N, ?R, ?W}
         end)).
-define(STATEBOX_EXPIRE, 60000).
-define(DEFAULT_TIMEOUT, 10000).


-record(resource_claim, {id :: fifo:uuid(),
			 ammount :: number()}).


-record(resource, {name :: fifo:resource_id(),
		   granted = 0:: number(),
		   claims = [] :: [fifo:resource_claim()],
		   reservations = [] :: [fifo:reservation()]}).

-record(snarl_obj, {val    :: val(),
		    vclock :: vclock:vclock()}).

-type val() ::  statebox:statebox().

-type snarl_obj() :: #snarl_obj{} | not_found.

-type idx_node() :: {integer(), node()}.

-type vnode_reply() :: {idx_node(), snarl_obj() | not_found}.
