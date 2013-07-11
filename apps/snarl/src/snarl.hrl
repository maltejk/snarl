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

-record(snarl_obj, {val    :: val(),
                    vclock :: vclock:vclock()}).

-type val()                ::  statebox:statebox().

-type snarl_obj()          :: #snarl_obj{} | not_found.

-type idx_node()           :: {integer(), node()}.

-type vnode_reply()        :: {idx_node(), snarl_obj() | not_found}.


-record(user_0_1_0, {
          uuid             :: vlwwregister:vlwwregister(),
          name             :: vlwwregister:vlwwregister(),
          password         :: vlwwregister:vlwwregister(),
          permissions      :: vorsetg:vorsetg(),
          groups           :: vorsetg:vorsetg(),
          metadata
         }).

-record(user_0_1_1, {
          uuid             :: vlwwregister:vlwwregister(),
          name             :: vlwwregister:vlwwregister(),
          password         :: vlwwregister:vlwwregister(),
          permissions      :: vorsetg:vorsetg(),
          groups           :: vorsetg:vorsetg(),
          ssh_keys         :: vorsetg:vorsetg(),
          metadata
         }).

-record(user_0_1_2, {
          uuid             :: vlwwregister:vlwwregister(),
          name             :: vlwwregister:vlwwregister(),
          password         :: vlwwregister:vlwwregister(),
          active_org       :: vlwwregister:vlwwregister(),
          permissions      :: vorsetg:vorsetg(),
          groups           :: vorsetg:vorsetg(),
          ssh_keys         :: vorsetg:vorsetg(),
          orgs             :: vorsetg:vorsetg(),
          metadata
         }).

-record(group_0_1_0, {
          uuid             :: vlwwregister:vlwwregister(),
          name             :: vlwwregister:vlwwregister(),
          permissions      :: vorsetg:vorsetg(),
          metadata
         }).

-record(organisation_0_1_0, {
          uuid             :: vlwwregister:vlwwregister(),
          name             :: vlwwregister:vlwwregister(),
          triggers         :: vorsetg:vorsetg(),
          metadata
         }).

-define(USER, user_0_1_2).
-define(GROUP, group_0_1_0).
-define(ORG, organisation_0_1_0).
