-define(ENV(K, D),
        (case application:get_env(snarl, K) of
             undefined ->
                 D;
             {ok, EnvValue} ->
                 EnvValue
         end)).

-record(vstate, {
          db,
          partition,
          service,
          bucket,
          service_bin,
          node,
          hashtrees,
          internal,
          state,
          vnode
         }).

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
-define(WEEK, 604800000000).
-define(DAY,   86400000000).
-define(HOUER,  3600000000).
-define(MINUTE,   60000000).
-define(SECOND,    1000000).


-define(STATEBOX_EXPIRE, 60000).
-define(DEFAULT_TIMEOUT, 10000).

-record(resource_claim,
        {id                :: fifo:uuid(),
         ammount           :: number()}).

-record(resource,
        {name              :: fifo:resource_id(),
         granted = 0       :: number(),
         claims = []       :: [fifo:resource_claim()],
         reservations = [] :: [fifo:reservation()]}).

-type idx_node()           :: {integer(), node()}.

-type vnode_reply()        :: {idx_node(), term() | not_found}.
