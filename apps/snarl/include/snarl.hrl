-record(vstate, {
          db,
          partition,
          service,
          bucket,
          node,
          hashtrees,
          internal,
          state,
          vnode,
          sync_tree
         }).

-define(N, application:get_env(snarl, n, 3)).
-define(R, application:get_env(snarl, r, 2)).
-define(W, application:get_env(snarl, w, 3)).
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
