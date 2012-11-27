-module(snarl_zmq_handler).

-include("snarl.hrl").

-export([init/1, message/2]).

-ignore_xref([init/1, message/2]).

init([]) ->
    {ok, stateless}.

%%%===================================================================
%%% User Functions
%%%===================================================================

message({user, list}, State) ->
    {reply, snarl_user:list(), State};


message({user, get, {token, Token}}, State) ->
    {ok, User} = snarl_token:get(Token),
    message({user, get, User}, State);

message({user, cache, {token, Token}}, State) ->
    {ok, User} = snarl_token:get(Token),
    message({user, cache, User}, State);

message({user, get, User}, State) ->
    {reply,
     snarl_user:get(ensure_binary(User)),
     State};

message({user, cache, User}, State) ->
    {reply,
     snarl_user:cache(ensure_binary(User)),
     State};

message({user, add, User}, State) ->
    {reply,
     snarl_user:add(ensure_binary(User)),
     State};

message({user, auth, User, Pass}, State) ->
    UserB = ensure_binary(User),
    Res = case snarl_user:auth(UserB, ensure_binary(Pass)) of
	      true ->
		  {ok, Token} = snarl_token:add(UserB),
		  {ok, {token, Token}};
	      Other ->
		  {error, Other}
	  end,
    {reply,
     Res,
     State};

message({user, allowed, {token, Token}, Permission}, State) ->
    {ok, User} = snarl_token:get(Token),
    {reply,
     snarl_user:allowed(User, Permission),
     State};

message({user, allowed, User, Permission}, State) ->
    {reply,
     snarl_user:allowed(ensure_binary(User), Permission),
     State};

message({user, delete, User}, State) ->
    {reply,
     snarl_user:delete(ensure_binary(User)),
     State};

message({user, passwd, User, Pass}, State) ->
    {reply,
     snarl_user:passwd(ensure_binary(User), ensure_binary(Pass)),
     State};

message({user, join, User, Group}, State) ->
    {reply, snarl_user:join(ensure_binary(User), Group), State};

message({user, leave, User, Group}, State) ->
    {reply, snarl_user:leave(ensure_binary(User), Group), State};

message({user, grant, User, Permission}, State) ->
    {reply, snarl_user:grant(ensure_binary(User), Permission), State};

message({user, revoke, User, Permission}, State) ->
    {reply, snarl_user:revoke(ensure_binary(User), Permission), State};

%%%===================================================================
%%% Resource Functions
%%%===================================================================

message({user, set_resource, User, Resource, Value}, State) ->
    {reply, snarl_user:set_resource(ensure_binary(User), Resource, Value), State};

%message({user, get_resource, User, Resource}, State) ->
%    {reply, snarl_user:get_resource(ensure_binary(User), Resource), State};

message({user, claim_resource, User, Resource, Ammount}, State) ->
    ID = uuid:uuid4(),
    {reply, {ID, snarl_user:claim_resource(ensure_binary(User), ID, Resource, Ammount)}, State};

message({user, free_resource, User, Resource, ID}, State) ->
    {reply, snarl_user:free_resource(ensure_binary(User), Resource, ID), State};

message({user, resource_stat, User}, State) ->
    {reply, snarl_user:get_resource_stat(ensure_binary(User)), State};

%%%===================================================================
%%% Group Functions
%%%===================================================================

message({group, list}, State) ->
    {reply, snarl_group:list(), State};

message({group, get, Group}, State) ->
    {reply, snarl_group:get(ensure_binary(Group)), State};

message({group, add, Group}, State) ->
    {reply, snarl_group:add(ensure_binary(Group)), State};

message({group, delete, Group}, State) ->
    {reply, snarl_group:delete(ensure_binary(Group)), State};

message({group, grant, Group, Permission}, State) ->
    {reply, snarl_group:grant(ensure_binary(Group), Permission), State};

message({group, revoke, Group, Permission}, State) ->
    {reply, snarl_group:revoke(ensure_binary(Group), Permission), State};

message(Message, State) ->
    io:format("Unsuppored 0MQ message: ~p", [Message]),
    {noreply, State}.

ensure_binary(A) when is_atom(A) ->
    list_to_binary(atom_to_list(A));
ensure_binary(L) when is_list(L) ->
    list_to_binary(L);
ensure_binary(B) when is_binary(B)->
    B;
ensure_binary(I) when is_integer(I) ->
    list_to_binary(integer_to_list(I));
ensure_binary(F) when is_float(F) ->
    list_to_binary(float_to_list(F));
ensure_binary(T) ->
    term_to_binary(T).
