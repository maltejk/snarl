-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

-define(N, 4).
-define(R, 3).
-define(W, 3).
-define(STATEBOX_EXPIRE, 60000).
-define(DEFAULT_TIMEOUT, 10000).


-record(user, {name :: binary(),
	       passwd :: binary(),
	       permissions = [] :: list(),
	       groups = [] :: list()}).

-type val()             ::  statebox:statebox().

-record(snarl_obj,        {val    :: val(),
			   vclock :: vclock:vclock()}).

-type snarl_obj()         :: #snarl_obj{} | not_found.

-type idx_node() :: {integer(), node()}.

-type vnode_reply() :: {idx_node(), snarl_obj() | not_found}.
