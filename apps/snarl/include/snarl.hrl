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
          roles            :: vorsetg:vorsetg(),
          metadata
         }).

-record(user_0_1_1, {
          uuid             :: vlwwregister:vlwwregister(),
          name             :: vlwwregister:vlwwregister(),
          password         :: vlwwregister:vlwwregister(),
          permissions      :: vorsetg:vorsetg(),
          roles            :: vorsetg:vorsetg(),
          ssh_keys         :: vorsetg:vorsetg(),
          metadata
         }).

-record(user_0_1_2, {
          uuid             :: vlwwregister:vlwwregister(),
          name             :: vlwwregister:vlwwregister(),
          password         :: vlwwregister:vlwwregister(),
          active_org       :: vlwwregister:vlwwregister(),
          permissions      :: vorsetg:vorsetg(),
          roles            :: vorsetg:vorsetg(),
          ssh_keys         :: vorsetg:vorsetg(),
          orgs             :: vorsetg:vorsetg(),
          metadata
         }).

-record(user_0_1_3, {
          uuid        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          name        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          password    = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          active_org  = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          permissions = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          roles       = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          ssh_keys    = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          orgs        = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          metadata    = riak_dt_map:new()    :: riak_dt_map:map()
         }).

-record(user_0_1_4, {
          uuid        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          name        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          password    = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          active_org  = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          permissions = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          roles       = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          ssh_keys    = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          orgs        = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          yubikeys    = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          metadata    = riak_dt_map:new()    :: riak_dt_map:map()
         }).

-record(user_0_1_5, {
          uuid        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          name        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          password    = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          active_org  = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          permissions = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          roles       = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          ssh_keys    = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          orgs        = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          yubikeys    = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          metadata    = riak_dt_map:new()    :: riak_dt_map:map()
         }).

-record(group_0_1_0, {
          uuid             :: vlwwregister:vlwwregister(),
          name             :: vlwwregister:vlwwregister(),
          permissions      :: vorsetg:vorsetg(),
          metadata
         }).

-record(group_0_1_1, {
          uuid        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          name        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          permissions = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          metadata    = riak_dt_map:new()    :: riak_dt_map:map()
         }).


-record(role_0_1_0, {
          uuid        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          name        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          permissions = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          metadata    = riak_dt_map:new()    :: riak_dt_map:map()
         }).

-record(organisation_0_1_0, {
          uuid             :: vlwwregister:vlwwregister(),
          name             :: vlwwregister:vlwwregister(),
          triggers         :: vorsetg:vorsetg(),
          metadata
         }).

-record(organisation_0_1_1, {
          uuid        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          name        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          triggers    = riak_dt_orswot:new() :: riak_dt_orswot:orswot(),
          metadata    = riak_dt_map:new()    :: riak_dt_map:map()
         }).

-record(organisation_0_1_2, {
          uuid        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          name        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          triggers    = riak_dt_map:new()    :: riak_dt_map:map(),
          metadata    = riak_dt_map:new()    :: riak_dt_map:map()
         }).

-record(organisation_0_1_3, {
          uuid        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          name        = riak_dt_lwwreg:new() :: riak_dt_lwwreg:lwwreg(),
          triggers    = riak_dt_map:new()    :: riak_dt_map:map(),
          metadata    = riak_dt_map:new()    :: riak_dt_map:map()
         }).

-define(USER, user_0_1_5).
-define(ROLE, role_0_1_0).
-define(ORG, organisation_0_1_3).


-define(NEW_LWW(V, T), riak_dt_lwwreg:update(
                         {assign, V, T}, none,
                         riak_dt_lwwreg:new())).

-define(CONVERT_VORSET(S),
        riak_dt_orswot:update(
          {add_all, vorsetg:value(S)}, none,
          riak_dt_orswot:new())).
