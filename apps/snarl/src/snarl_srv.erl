%%%-------------------------------------------------------------------
%%% @author Heinz N. Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz N. Gies
%%% @doc
%%%
%%% @end
%%% Created :  8 May 2012 by Heinz N. Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(snarl_srv).

-behaviour(gen_server).

%% API
-export([start_link/0, match/2, call/2, initialize/0, initialize/2, reregister/0,
	initialize_groups/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

call(Auth, Call) ->
    gen_server:call(?SERVER, {call, Auth, Call}).

initialize() ->
    initialize(<<"admin">>, <<"admin">>).

initialize(Name, Pass) ->
    gen_server:call(?SERVER, {init, Name, Pass}).

initialize_groups() ->
    gen_server:cast(?SERVER, init_groups).


reregister() ->
    gen_server:cast(?SERVER, reregister).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    ok = backyard_srv:register_connect_handler(backyard_connect),
    case redo:cmd([<<"SMEMBERS">>, <<"fifo:groups">>]) of
	[] ->
	    int_init_groups();
	_ ->
	    ok
    end,
    case redo:cmd([<<"SMEMBERS">>, <<"fifo:users">>]) of
	[] ->
	    int_init(<<"admin">>, <<"admin">>);
	_ ->
	    ok
    end,
    {ok, #state{}, 1000}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_call({init, Name, Pass}, _From, State) ->
    UUID = int_init(Name, Pass),
    {reply, {ok, UUID}, State};

handle_call({call, Auth, {user, list}}, _From, State) ->
    case test_user(Auth, [user, list]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true ->
	    Res = redo:cmd([<<"SMEMBERS">>, <<"fifo:users">>]),
	    {reply, {ok, Res}, State}
    end;

handle_call({call, Auth, {user, add, Name, Pass}}, _From, State) ->
    case test_user(Auth, [user, add]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true ->
	    case redo:cmd([<<"GET">>, <<"fifo:users:", Name/binary>>]) of
		undefined ->
		    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
		    Hash = crypto:sha256(<<Name/binary, ":", Pass/binary>>),
		    redo:cmd([<<"SADD">>, <<"fifo:users">>, Name]),
		    redo:cmd([<<"SET">>, <<"fifo:users:", Name/binary>>, UUID]),
		    redo:cmd([<<"SET">>, <<"fifo:users:", UUID/binary, ":hash">>, Hash]),
		    redo:cmd([<<"SET">>, <<"fifo:users:", UUID/binary, ":name">>, Name]),
		    redo:cmd([<<"SADD">>, <<"fifo:users:", UUID/binary, ":permissions">>, term_to_binary([user, UUID, '...'])]),
		    {reply, {ok, UUID}, State};
		_ -> 
		    {reply, {error, user_exists}, State}
	    end
    end;


handle_call({call, Auth, {user, passwd, UUID, Pass}}, _From, State) ->
    case test_user(Auth, [user, UUID, passwd]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true->
	    case redo:cmd([<<"GET">>, <<"fifo:users:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		Name ->
		    Hash = crypto:sha256(<<Name/binary, ":", Pass/binary>>),
		    redo:cmd([<<"SET">>, <<"fifo:users:", UUID/binary, ":hash">>, Hash]),
		    {reply, ok, State}
	    end
    end;

handle_call({call, Auth, {user, delete, UUID}}, _From, State) ->
    case test_user(Auth, [user, UUID, delete]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true->
	    case redo:cmd([<<"GET">>, <<"fifo:users:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		Name ->
		    [delete_user_from_group(UUID, GUUID) ||
			GUUID <- redo:cmd([<<"SMEMBERS">>, <<"fifo:users:", UUID/binary, ":groups">>])],
		    redo:cmd([<<"DEL">>, 
			      <<"fifo:users:", Name/binary>>,
			      <<"fifo:users:", UUID/binary, ":hash">>,
			      <<"fifo:users:", UUID/binary, ":name">>,
			      <<"fifo:users:", UUID/binary, ":permissions">>]),
		    redo:cmd([<<"SREM">>, <<"fifo:users">>, Name]),
		    {reply, ok, State}
	    end
    end;

handle_call({call, _Auth, {user, auth, Name, Pass}}, _From, State) ->
    Hash = crypto:sha256(<<Name/binary, ":", Pass/binary>>),
    case redo:cmd([<<"GET">>, <<"fifo:users:", Name/binary>>]) of
	undefined ->
	    {reply, {error, not_found}, State};
	UUID ->
	    
	    case redo:cmd([<<"GET">>, <<"fifo:users:", UUID/binary, ":hash">>]) of
		Hash ->
		    {reply, {ok, UUID}, State};
		_ ->
		    {reply, {error, not_found}, State}
	    end
    end;	 

handle_call({call, Auth, {user, get, Name}}, _From, State) ->
    case test_user(Auth, [user, get, Name]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true->
	    case redo:cmd([<<"GET">>, <<"fifo:users:", Name/binary>>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		UUID ->
		    {reply, {ok, UUID}, State}
	    end
    end;

handle_call({call, Auth, {user, groups, UUID}}, _From, State) ->
    case test_user(Auth, [user, groups, UUID]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true->
	    case redo:cmd([<<"SMEMBERS">>, <<"fifo:users:", UUID/binary, ":groups">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		Res ->
		    {reply, {ok, Res}, State}
	    end
    end;
    

handle_call({call, Auth, {user, permissions, UUID}}, _From, State) ->
    case test_user(Auth, [user, UUID, permissions]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true->
	    case redo:cmd([<<"GET">>, <<"fifo:users:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		_Name ->
		    {reply, {ok, user_permissions(UUID)}, State}
	    end
    end;

handle_call({call, Auth, {user, own_permissions, UUID}}, _From, State) ->
    case test_user(Auth, [user, UUID, own_permissions]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true->
	    case redo:cmd([<<"GET">>, <<"fifo:users:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		_Name ->
		    Perms = [binary_to_term(T) || 
				T <- redo:cmd([<<"SMEMBERS">>, <<"fifo:users:", UUID/binary, ":permissions">>])],
		    {reply, {ok, Perms}, State}
	    end
    end;

handle_call({call, Auth, {user, name, UUID}}, _From, State) ->
    case test_user(Auth, [user, UUID, name]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true->
	    case redo:cmd([<<"GET">>, <<"fifo:users:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		Name ->
		    {reply, {ok, Name}, State}
	    end
    end;

handle_call({call, Auth, {user, allowed, UUID, Perm}}, _From, State) ->
    case test_user(Auth, [user, UUID, allowed]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true->
	    case redo:cmd([<<"GET">>, <<"fifo:users:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		_Name ->
		    {reply, test_user(UUID, Perm), State}
	    end
    end;

handle_call({call, Auth, {user, groups, add, UUUID, GUUID}}, _From, State) ->
    case {test_user(Auth, [user, UUUID, groups, add, GUUID]), test_user(Auth, [group, GUUID, groups, add, UUUID])} of
	{true, true} ->
	    {reply, add_user_to_group(UUUID, GUUID), State};
	_ ->
	    {reply, {error, permission_denied}, State}
    end;

handle_call({call, Auth, {user, groups, delete, UUUID, GUUID}}, _From, State) ->
    case {test_user(Auth, [user, UUUID, groups, delete]), test_user(Auth, [group, GUUID, groups, delete, UUUID])} of
	{true, true} ->
	    {reply, delete_user_from_group(UUUID, GUUID), State};
	_ ->
	    {reply, {error, permission_denied}, State}
    end;

handle_call({call, Auth, {user, grant, UUID, Perm}}, _From, State) ->
    case {test_user(Auth, [user, UUID, grant]), test_user(Auth, [permissions, user, grant] ++ Perm)} of
	{true, true} ->
	    {reply, user_grant(UUID, Perm), State};
	_ ->
	    {reply, {error, permission_denied}, State}
    end;

handle_call({call, Auth, {user, revoke, UUID, Perm}}, _From, State) ->
    case {test_user(Auth, [user, UUID, revoke]), test_user(Auth, [permissions, user, revoke] ++ Perm)} of
	{true, true} ->
	    case redo:cmd([<<"GET">>, <<"fifo:users:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		_Name ->
		    redo:cmd([<<"SREM">>, <<"fifo:users:", UUID/binary, ":permissions">>, term_to_binary(Perm)]),
		    {reply, ok, State}
	    end;
	_ ->
	    {reply, {error, permission_denied}, State}
    end;

handle_call({call, Auth, {group, list}}, _From, State) ->
    case test_user(Auth, [group, list]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true ->
	    Res = redo:cmd([<<"SMEMBERS">>, <<"fifo:groups">>]),
	    {reply, {ok, Res}, State}
    end;

handle_call({call, Auth, {group, users, UUID}}, _From, State) ->
    case test_user(Auth, [group, users, UUID]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true->
	    case redo:cmd([<<"SMEMBERS">>, <<"fifo:groups:", UUID/binary, ":users">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		Res ->
		    {reply, {ok, Res}, State}
	    end
    end;


handle_call({call, Auth, {group, add, Name}}, _From, State) ->
    io:format("group add~n"),
    case test_user(Auth, [group, add]) of
	true ->
	    {reply, group_add(Name), State};
	false ->
	    {reply, {error, permission_denied}, State}
    end;

handle_call({call, Auth, {group, delete, UUID}}, _From, State) ->
    case test_user(Auth, [group, UUID, delete]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true->
	    case redo:cmd([<<"GET">>, <<"fifo:groups:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		Name ->
		    [delete_user_from_group(UUUID, UUID) ||
			UUUID <- redo:cmd([<<"SMEMBERS">>, <<"fifo:groups:", UUID/binary, ":users">>])],
		    redo:cmd([<<"SREM">>, <<"fifo:groups">>, Name]),
		    redo:cmd([<<"DEL">>, 
			      <<"fifo:groups:", Name/binary>>,
			      <<"fifo:groups:", UUID/binary, ":name">>]),
		    {reply, ok, State}
	    end
    end;

handle_call({call, Auth, {group, get, Name}}, _From, State) ->
    case test_user(Auth, [group, get, Name]) of
	true ->	    
	    case redo:cmd([<<"GET">>, <<"fifo:groups:", Name/binary>>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		UUID ->
		    {reply, {ok, UUID}, State}
	    end;
	false ->
	    {reply, {error, permission_denied}, State}
    end;

handle_call({call, Auth, {group, permissions, UUID}}, _From, State) ->
    case test_user(Auth, [group, UUID, permissions]) of
	false ->
	    {reply, {error, permission_denied}, State};
	true->
	    case redo:cmd([<<"GET">>, <<"fifo:groups:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		_Name ->
		    Perms = [binary_to_term(T) || 
				T <- redo:cmd([<<"SMEMBERS">>, <<"fifo:groups:", UUID/binary, ":permissions">>])],
		    {reply, {ok, Perms}, State}
	    end
    end;

handle_call({call, Auth, {group, name, UUID}}, _From, State) ->
    case test_user(Auth, [group, UUID, name]) of
	true ->	    
	    case redo:cmd([<<"GET">>, <<"fifo:groups:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		Name ->
		    {reply, {ok, Name}, State}
	    end;
	false ->
	    {reply, {error, permission_denied}, State}
    end;


handle_call({call, Auth, {group, users, add, GUUID, UUUID}}, _From, State) ->
    case {test_user(Auth, [user, UUUID, groups, add, GUUID]), test_user(Auth, [group, GUUID, groups, add, UUUID])} of
	{true, true} ->
	    {reply, add_user_to_group(UUUID, GUUID), State};
	_ ->
	    {reply, {error, permission_denied}, State}
    end;

handle_call({call, Auth, {group, users, delete, GUUID, UUUID}}, _From, State) ->
    case {test_user(Auth, [user, UUUID, groups, delete]), test_user(Auth, [group, GUUID, groups, delete, UUUID])} of
	{true, true} ->
	    {reply, delete_user_from_group(UUUID, GUUID), State};
	_ ->
	    {reply, {error, permission_denied}, State}
    end;

handle_call({call, Auth, {group, grant, UUID, Perm}}, _From, State) ->
    case {test_user(Auth, [group, UUID, grant]), test_user(Auth, [permissions, group, grant] ++ Perm)} of
	{true, true} ->
	    case redo:cmd([<<"GET">>, <<"fifo:groups:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		_Name ->
		    redo:cmd([<<"SADD">>, <<"fifo:groups:", UUID/binary, ":permissions">>, term_to_binary(Perm)]),
		    {reply, ok, State}
	    end;
	_ ->
	    {reply, {error, permission_denied}, State}
    end;


handle_call({call, Auth, {group, revoke, UUID, Perm}}, _From, State) ->
    case {test_user(Auth, [group, UUID, revoke]), test_user(Auth, [permissions, group, revoke] ++ Perm)} of
	{true, true} ->
	    case redo:cmd([<<"GET">>, <<"fifo:groups:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		_Name ->
		    redo:cmd([<<"SREM">>, <<"fifo:groups:", UUID/binary, ":permissions">>, term_to_binary(Perm)]),
		    {reply, ok, State}
	    end;
	_ ->
	    {reply, {error, permission_denied}, State}
    end;

handle_call({call, Auth, {option, list, Category}}, _From, State) ->
    case test_user(Auth, [option, Category, list]) of
	false -> 	    
	    {reply, {error, permission_denied}, State};
	true ->
	    C = ensure_binary(Category),
	    case redo:cmd([<<"SMEMBERS">>, <<"fifo:options:", C/binary>>]) of
		undefined ->
		    {reply, {ok, []}, State};
		Res ->
		    {reply, {ok, [binary_to_term(R)||R<-Res]}, State}
	    end
    end;

handle_call({call, Auth, {option, get, Category, Name}}, _From, State) ->
    case test_user(Auth, [option, Category, get, Name]) of
	false -> 	    
	    {reply, {error, permission_denied}, State};
	true ->
	    C = ensure_binary(Category),
	    N = ensure_binary(Name),
	    case redo:cmd([<<"GET">>, <<"fifo:options:", C/binary, ":", N/binary>>]) of
		undefined ->
		    {reply, undefined, State};
		Res ->
		    {reply, {ok, binary_to_term(Res)}, State}
	    end
    end;

handle_call({call, Auth, {option, delete, Category, Name}}, _From, State) ->
    case test_user(Auth, [option, Category, delete, Name]) of
	false -> 	    
	    {reply, {error, permission_denied}, State};
	true ->
	    C = ensure_binary(Category),
	    N = ensure_binary(Name),
	    redo:cmd([<<"SDEL">>, <<"fifo:options:", C/binary>>, N]),
	    redo:cmd([<<"DEL">>, <<"fifo:options:", C/binary, ":", N/binary>>]),
	    {reply, ok, State}
    end;

handle_call({call, Auth, {option, set, Category, Name, Value}}, _From, State) ->
    case test_user(Auth, [option, Category, set, Name]) of
	false -> 	    
	    {reply, {error, permission_denied}, State};
	true ->
	    C = ensure_binary(Category),
	    N = ensure_binary(Name),
	    V = term_to_binary(Value),
	    redo:cmd([<<"SADD">>, <<"fifo:options:", C/binary>>, term_to_binary(Name)]),
	    redo:cmd([<<"SET">>,  <<"fifo:options:", C/binary, ":", N/binary>>, V]),
	    {reply, ok, State}
    end;

handle_call({call, Auth, {network, add, Name, First, Netmask, Gateway}}, _From, State) ->
    case test_user(Auth, [network, create]) of
	false -> 	    
	    {reply, {error, permission_denied}, State};
	true ->
	    case redo:cmd([<<"GET">>, <<"fifo:networks:", Name/binary>>]) of
		undefined ->
		    Network = First band Netmask,
		    redo:cmd([<<"SADD">>, <<"fifo:networks">>, term_to_binary(Name)]),
		    redo:cmd([<<"SET">>, <<"fifo:networks:", Name/binary>>, term_to_binary({Network, First, Netmask, Gateway})]),
		    {reply, ok, State};
		_ ->
		    {reply, {error, already_exists}, State}		
	    end
    end;

handle_call({call, Auth, {network, delete, Name}}, _From, State) ->
    Res = case test_user(Auth, [network, Name, delete]) of
	      false -> 	    
		  {reply, {error, permission_denied}, State};
	      true ->
		  case redo:cmd([<<"GET">>, <<"fifo:networks:", Name/binary>>]) of
		      undefined ->
			  {error, not_found};
		      _ ->
			  redo:cmd([<<"SREM">>, <<"fifo:networks">>, term_to_binary(Name)]),	  
			  redo:cmd([<<"DEL">>, <<"fifo:networks:", Name/binary>>]),
			  redo:cmd([<<"DEL">>, <<"fifo:networks:", Name/binary, ":free">>]),
			  ok
		  end
	  end,
    {reply, Res, State};

handle_call({call, Auth, {network, get, Name}}, _From, State) ->
    Res = case test_user(Auth, [network, Name, get]) of
	      false ->
		  {error, permission_denied};
	      true ->
		  case redo:cmd([<<"GET">>, <<"fifo:networks:", Name/binary>>]) of
		      undefined ->
			  {error, not_found};
		      Bin ->
			  {Network, First, Netmask, Gateway} = binary_to_term(Bin),
			  case redo:cmd([<<"LPOP">>, <<"fifo:networks:", Name/binary, ":free">>]) of
			      undefined ->
				  {ok, {Network, Netmask, Gateway, First}};
			      Free ->
				  redo:cmd([<<"LPUSH">>, <<"fifo:networks:", Name/binary, ":free">>, Free]),
				  {ok, {Network, Netmask, Gateway, binary_to_term(Free)}}
			  end
		  end
	  end,
    {reply, Res, State};

handle_call({call, Auth, {network, get, ip, Name}}, _From, State) ->
    Res = case test_user(Auth, [network, Name, next_ip]) of
	      false ->
		  {error, permission_denied};
	      true ->
		  case redo:cmd([<<"GET">>, <<"fifo:networks:", Name/binary>>]) of
		      undefined ->
			  {error, not_found};
		      Bin ->
			  IP = case redo:cmd([<<"LPOP">>, <<"fifo:networks:", Name/binary, ":free">>]) of
				   undefined ->
				       {Network, First, Netmask, Gateway} = binary_to_term(Bin),
				       redo:cmd([<<"SET">>, <<"fifo:networks:", Name/binary>>, 
						 term_to_binary({Network, First+1, Netmask, Gateway})]),
				       First;
				   Free ->
				       binary_to_term(Free)
			       end,
			  {ok, IP}
		  end
	  end,
    {reply, Res, State};

handle_call({call, Auth, {network, release, ip, Name, IP}}, _From, State) ->
    IPStr = ip_to_str(IP),
    lager:info([{fifi_component, snarl}],
	       "network:release - Network: ~s, IP: ~s.", [Name, IPStr]),
    Res = case test_user(Auth, [network, Name, release, IPStr]) of
	      false ->
		  lager:warning([{fifi_component, snarl}],
				"network:releas - forbidden Auth: ~p, Perm: ~p.", [Auth, [network, Name, release, IPStr]]),
		  {error, permission_denied};
	      true ->
		  case redo:cmd([<<"GET">>, <<"fifo:networks:", Name/binary>>]) of
		      undefined ->
			  {error, not_found};
		      _Bin ->
			  redo:cmd([<<"LPUSH">>, <<"fifo:networks:", Name/binary, ":free">>, term_to_binary(IP)]),
			  ok
		  end
	  end,
    {reply, Res, State};


handle_call({call, Auth, {network, get, What, Name}}, _From, State) ->
    {reply,
     net_get(Auth, Name, What),
     State};

handle_call(_Request, _From, State) ->
    Reply = {error, not_implemented},
    {reply, Reply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(backyard_connect, State) ->
    gproc:reg({n, g, snarl}),
    {noreply, State};
handle_cast(init_groups, State) ->
    int_init_groups(),
    {noreply, State};
handle_cast(reregister, State) ->
    try
%	gproc:reg({n, g, snarl}),
	{noreply, State}
    catch
	_T:_E ->
	    {noreply, State, 1000}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(timeout, State) ->
    reregister(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


match([], []) ->
    lager:info("snarl:match - Direct match"),
    true;

match(P, ['...']) ->
    lager:info("snarl:match - Matched ~p by '...'", [P]),
    true;

match([], ['...'|_Rest]) ->
    false;

match([], [_X|_R]) ->
    false;

match([X | InRest], ['...', X|TestRest] = Test) ->
    match(InRest, TestRest) orelse match(InRest, Test);

match([_,X|InRest], ['...', X|TestRest] = Test) ->
    match(InRest, TestRest) orelse match([X| InRest], Test);

match([X|InRest], [X|TestRest]) ->
    match(InRest, TestRest);

match([_|InRest], ['_'|TestRest]) ->
    match(InRest, TestRest);

match(_, _) ->
    false.

test_perms(_Perm, []) ->
    false;

test_perms(Perm, [Test|Tests]) ->
    match(Perm, Test) orelse test_perms(Perm, Tests).

test_user(system, _Perm) ->
    lager:info("snarl:match - Matched by system user."),
    true;

test_user(UUID, Perm) ->
    test_perms(Perm, user_permissions(UUID)).


user_permissions(UUID) ->
    Groups = [  <<"fifo:groups:", G/binary, ":permissions">> || 
		 G <- redo:cmd(["SMEMBERS", <<"fifo:users:", UUID/binary, ":groups">>])],
    [binary_to_term(T) || 
	T <-  redo:cmd([<<"SUNION">>, <<"fifo:users:", UUID/binary, ":permissions">>] ++ Groups)].

add_user_to_group(UUUID, GUUID) ->
    case redo:cmd([<<"GET">>, <<"fifo:users:", UUUID/binary, ":name">>]) of
	undefined ->
	    {error, {not_found, user}};
	_ ->
	    case redo:cmd([<<"GET">>, <<"fifo:groups:", GUUID/binary, ":name">>]) of
		undefined ->
		    {error, {not_found, group}};
		_ -> 
		    redo:cmd([<<"SADD">>, <<"fifo:users:", UUUID/binary, ":groups">>, GUUID]),
		    redo:cmd([<<"SADD">>, <<"fifo:groups:", GUUID/binary, ":users">>, UUUID]),
		    ok
	    end
    end.

delete_user_from_group(UUUID, GUUID) ->
    case redo:cmd([<<"GET">>, <<"fifo:users:", UUUID/binary, ":name">>]) of
	undefined ->
	    {error, {not_found, user}};
	_ ->
	    case redo:cmd([<<"GET">>, <<"fifo:groups:", GUUID/binary, ":name">>]) of
		undefined ->
		    {error, {not_found, group}};
		_ -> 
		    redo:cmd([<<"SREM">>, <<"fifo:users:", UUUID/binary, ":groups">>, GUUID]),
		    redo:cmd([<<"SREM">>, <<"fifo:groups:", GUUID/binary, ":users">>, UUUID]),
		    ok
	    end
    end.

ensure_binary(L) when is_list(L) ->
    list_to_binary(L);
ensure_binary(A) when is_atom(A) ->
    list_to_binary(atom_to_list(A));
ensure_binary(B) when is_binary(B) ->
    B;
ensure_binary(T) ->
    term_to_binary(T).



    
group_add(Name) ->
    case redo:cmd([<<"GET">>, <<"fifo:groups:", Name/binary>>]) of
	undefined ->
	    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
	    redo:cmd([<<"SET">>, <<"fifo:groups:", Name/binary>>, UUID]),
	    redo:cmd([<<"SET">>, <<"fifo:groups:", UUID/binary, ":name">>, Name]),
	    redo:cmd([<<"SADD">>, <<"fifo:groups">>, Name]),
	    io:format("UUID: ~p~n", [UUID]),
	    {ok, UUID};
	UUID ->
	    {error, exists, UUID}
    end.

group_grant(UUID, Perm) ->
    case redo:cmd([<<"GET">>, <<"fifo:groups:", UUID/binary, ":name">>]) of
	undefined ->
	    {error, not_found};
	_Name ->
	    redo:cmd([<<"SADD">>, <<"fifo:groups:", UUID/binary, ":permissions">>, term_to_binary(Perm)]),
	    ok
    end.

user_grant(UUID, Perm) ->
    io:format("user_grant(~p, ~p)~n", [UUID, Perm]),
    case redo:cmd([<<"GET">>, <<"fifo:users:", UUID/binary, ":name">>]) of
	undefined ->
	    {error, not_found};
	_Name ->
	    redo:cmd([<<"SADD">>, <<"fifo:users:", UUID/binary, ":permissions">>, term_to_binary(Perm)]),
	    ok
    end.

net_get(Auth, Name, What) ->
    case test_user(Auth, [network, Name, get]) of
	false -> 	    
	    {error, permission_denied};
	true ->
	    case redo:cmd([<<"GET">>, <<"fifo:networks:", Name/binary>>]) of
		undefined ->
		    {error, not_found};
		Bin ->
		    {Network, _First, Netmask, Gateway} = binary_to_term(Bin),
		    case What of
			net ->
			    {ok, Network};
			mask ->
			    {ok, Netmask};
			gateway ->
			    {ok, Gateway};
			_ ->
			    {error, not_implemented}
		    end
	    end
    end.


int_init(Name, Pass) ->
    UUID = list_to_binary(uuid:to_string(uuid:uuid4())),
    Hash = crypto:sha256(<<Name/binary, ":", Pass/binary>>),
    redo:cmd([<<"SADD">>, <<"fifo:users">>, Name]),
    redo:cmd([<<"SET">>, <<"fifo:users:", Name/binary>>, UUID]),
    redo:cmd([<<"SET">>, <<"fifo:users:", UUID/binary, ":hash">>, Hash]),
    redo:cmd([<<"SET">>, <<"fifo:users:", UUID/binary, ":name">>, Name]),
    redo:cmd([<<"SADD">>, <<"fifo:users:", UUID/binary, ":permissions">>, term_to_binary(['...'])]),
    UUID.



int_init_groups() ->
    io:format("1~n"),
    {ok, Admins} = group_add(<<"admins">>),
    [group_grant(Admins, Perm) ||
	Perm <- [['...']]],
   
    {ok, Users} = group_add(<<"users">>),
    io:format("2: ~p~n", [Users]),
    [group_grant(Users, Perm) ||
	Perm <- [[service, wiggle, module, about],
		 [service, wiggle, login],
		 [service, wiggle, module, account],
		 [service, wiggle, module, analytics],
		 [service, wiggle, module, home],
		 [service, wiggle, module, system],
		 [service, wiggle, module, event],
		 [vm, create],
		 [service, sniffle, info],
		 [network, '_', next_ip],
		 [dataset, '_', get],
		 [package, '_', get],
		 [package, list]]],
    {ok, UsersAdmins} = group_add(<<"user_admins">>),
    [group_grant(UsersAdmins, Perm) ||
	Perm <- [[service, wiggle, module, admin],
		 [user, '...'],
		 [group, '...']]],
    {ok, PackageAdmins} = group_add(<<"package_admins">>),
    [group_grant(PackageAdmins, Perm) ||
	Perm <- [[service, wiggle, module, home],
		 [package, '...']]],
    {ok, NetworkAdmins} = group_add(<<"network_admins">>),
    [group_grant(NetworkAdmins, Perm) ||
	Perm <- [[service, wiggle, module, admin],
		 [network, '...']]].

ip_to_str(IP) when is_integer(IP) ->
    ip_to_str(<<IP:32>>);
ip_to_str(<<A:8, B:8, C:8, D:8>>) ->
    list_to_binary(io_lib:format("~p.~p.~p.~p", [A, B, C, D])).
