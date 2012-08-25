-module(snarl_zmq_handler).


-export([init/1, message/2]).

init([]) ->
    {ok, stateless}.




%%%===================================================================
%%% User Functions
%%%===================================================================

message({user, list}, State) ->
    {reply, snarl_user:list(), State};

message({user, get, User}, State) ->
    {reply, snarl_user:get(User), State};

message({user, add, User}, State) ->
    {reply, snarl_user:add(User), State};

message({user, auth, User, Pass}, State) ->
    {reply, snarl_user:auth(User, Pass), State};

message({user, allowed, User, Permission}, State) ->
    {reply, snarl_user:allowed(User, Permission), State};

message({user, delete, User}, State) ->
    {reply, snarl_user:delete(User), State};

message({user, passwd, User, Pass}, State) ->
    {reply, snarl_user:passwd(User, Pass), State};

message({user, join, User, Group}, State) ->
    {reply, snarl_user:join(User, Group), State};

message({user, leave, User, Group}, State) ->
    {reply, snarl_user:leave(User, Group), State};

message({user, grant, User, Permission}, State) ->
    {reply, snarl_user:grant(User, Permission), State};

message({user, revoke, User, Permission}, State) ->
    {reply, snarl_user:grant(User, Permission), State};

%%%===================================================================
%%% Group Functions
%%%===================================================================

message({group, list}, State) ->
    {reply, snarl_group:list(), State};

message({group, get, Group}, State) ->
    {reply, snarl_group:get(Group), State};

message({group, add, Group}, State) ->
    {reply, snarl_group:add(Group), State};

message({group, delete, Group}, State) ->
    {reply, snarl_group:delete(Group), State};

message({group, grant, Group, Permission}, State) ->
    {reply, snarl_group:grant(Group, Permission), State};

message({group, revoke, Group, Permission}, State) ->
    {reply, snarl_group:grant(Group, Permission), State};

message(Message, State) ->
    io:format("Unsuppored 0MQ message: ~p", [Message]),
    {noreply, State}.
