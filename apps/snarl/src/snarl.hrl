%-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(PRINT(Var), 1 == 1 orelse io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

-define(N, 3).
-define(R, 2).
-define(W, 3).
-define(STATEBOX_EXPIRE, 60000).
-define(DEFAULT_TIMEOUT, 10000).


-record(resource_claim, {id :: binary(),
			 ammount :: number()}).


-record(resource, {name :: binary(),
		   granted = 0:: number(),
		   claims = [] :: [resource_claim()],
		   reservations = [] :: [reservation()]}).

-record(user, {name :: binary(),
	       passwd :: binary(),
	       permissions = [] :: [permission()],
	       resources = [] :: [resource()],
	       groups = [] :: [binary()]}).

-record(group, {name :: binary(),
		permissions = [] :: [permission()],
		users = [] :: [binary()]}).

-record(snarl_obj, {val    :: val(),
		    vclock :: vclock:vclock()}).

-type reservation() :: {resource_claim(), integer()}.

-type resource() :: #resource{}.

-type permission() :: [binary() | atom()].

-type resource_claim() :: #resource_claim{}.

-type val() ::  statebox:statebox().

-type snarl_obj() :: #snarl_obj{} | not_found.

-type idx_node() :: {integer(), node()}.

-type vnode_reply() :: {idx_node(), snarl_obj() | not_found}.
