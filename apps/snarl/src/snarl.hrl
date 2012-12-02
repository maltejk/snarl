%-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(PRINT(Var), 1 == 1 orelse io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

-define(N, 3).
-define(R, 2).
-define(W, 3).
-define(STATEBOX_EXPIRE, 60000).
-define(DEFAULT_TIMEOUT, 10000).


-record(resource_claim, {id :: fifo:uuid(),
			 ammount :: number()}).


-record(resource, {name :: fifo:resource_id(),
		   granted = 0:: number(),
		   claims = [] :: [fifo:resource_claim()],
		   reservations = [] :: [fifo:reservation()]}).

-record(user, {name :: fifo:user_id(),
	       passwd :: binary(),
	       permissions = [] :: [fifo:permission()],
	       resources = [] :: [fifo:resource()],
	       groups = [] :: [fifo:group_id()]}).

-record(group, {name :: fifo:group_id(),
		permissions = [] :: [fifo:permission()],
		users = [] :: [fifo:user_id()]}).

-record(snarl_obj, {val    :: val(),
		    vclock :: vclock:vclock()}).

-type val() ::  statebox:statebox().

-type snarl_obj() :: #snarl_obj{} | not_found.

-type idx_node() :: {integer(), node()}.

-type vnode_reply() :: {idx_node(), snarl_obj() | not_found}.
