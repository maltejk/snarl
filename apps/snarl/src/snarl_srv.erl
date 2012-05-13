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
-export([start_link/0, match/2, call/2, initialize/0, initialize/2, reregister/0]).

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
    UUID = uuid:uuid4(),
    Hash = crypto:sha256(<<Name/binary, ":", Pass/binary>>),
    redo:cmd([<<"SET">>, <<"fifo:users:", Name/binary>>, UUID]),
    redo:cmd([<<"SET">>, <<"fifo:users:", UUID/binary, ":hash">>, Hash]),
    redo:cmd([<<"SET">>, <<"fifo:users:", UUID/binary, ":name">>, Name]),
    redo:cmd([<<"SADD">>, <<"fifo:users:", UUID/binary, ":permissions">>, term_to_binary(['...'])]),
    {reply, {ok, UUID}, State};

handle_call({call, Auth, {user, add, Name, Pass}}, _From, State) ->
    case test_user(Auth, [user, add]) of
	false ->
	    {reply, {error, permission_denied}, State};
	ture ->
	    case redo:cmd([<<"GET">>, <<"fifo:users:", Name/binary>>]) of
		undefined ->
		    UUID = uuid:uuid4(),
		    Hash = crypto:sha256(<<Name/binary, ":", Pass/binary>>),
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
	    case redo:cmd([<<"GET">>, <<"fifo:users:", UUID/binary, ":name">>]) of
		undefined ->
		    {reply, {error, not_found}, State};
		_Name ->
		    redo:cmd([<<"SADD">>, <<"fifo:users:", UUID/binary, ":permissions">>, term_to_binary(Perm)]),
		    {reply, ok, State}
	    end;
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

handle_call({call, Auth, {group, add, Name}}, _From, State) ->
    case test_user(Auth, [group, add]) of
	true ->
	    case redo:cmd([<<"GET">>, <<"fifo:groups:", Name/binary>>]) of
		undefined ->
		    UUID = uuid:uuid4(),
		    redo:cmd([<<"SET">>, <<"fifo:groups:", Name/binary>>, UUID]),
		    redo:cmd([<<"SET">>, <<"fifo:groups:", UUID/binary, ":name">>, Name]),
		    {reply, {ok, UUID}, State};
		UUID ->
		    {reply, {error, exists, UUID}, State}
	    end;
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
		    redo:cmd([<<"DEL">>, 
			      <<"fifo:users:", Name/binary>>,
			      <<"fifo:users:", UUID/binary, ":name">>]),
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
		UUID ->
		    {reply, {ok, redo:cmd([<<"SMEMBERS">>, <<"fifo:groups:", UUID/binary, ":permissions">>])}, State}
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
handle_cast(reregister, State) ->
    gproc:reg({n, g, snarl}),
    {noreply, State};
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
    true;

match(_, ['...']) ->
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
