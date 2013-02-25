-module(snarl_tcp_handler).

-include("snarl.hrl").

-include("snarl_version.hrl").

-export([init/2, message/2]).

-ignore_xref([init/2, message/2]).

-record(state, {port}).

init(Prot, []) ->
    {ok, #state{port = Prot}}.

%%%===================================================================
%%% User Functions
%%%===================================================================

-spec message(fifo:smarl_message(), term()) -> any().

message(version, State) ->
    {stop, normal, ?VERSION, State};

message({user, list}, State) ->
    {stop, normal, snarl_user:list(), State};

message({user, get, {token, Token}}, State) ->
    case snarl_token:get(Token) of
        {ok, not_found} ->
            {stop, normal, not_found, State};
        {ok, User} ->
            message({user, get, User}, State)
    end;

message({user, cache, {token, Token}}, State) ->
    case snarl_token:get(Token) of
        {ok, not_found} ->
            {stop, normal, not_found, State};
        {ok, User} ->
            message({user, cache, User}, State)
    end;

message({user, get, User}, State) when
      is_binary(User) ->
    {stop, normal,
     snarl_user:get(User),
     State};

message({user, set, User, Attribute, Value}, State) when
      is_binary(User) ->
    {stop, normal,
     snarl_user:set(User, Attribute, Value),
     State};

message({user, set, User, Attributes}, State) when
      is_binary(User) ->
    {stop, normal,
     snarl_user:set(User, Attributes),
     State};

message({user, lookup, User}, State) when is_binary(User) ->
    {stop, normal,
     snarl_user:lookup(User),
     State};

message({user, cache, User}, State) ->
    {stop, normal,
     snarl_user:cache(ensure_binary(User)),
     State};

message({user, add, User}, State) ->
    {stop, normal,
     snarl_user:add(ensure_binary(User)),
     State};

message({user, auth, User, Pass}, State) ->
    UserB = ensure_binary(User),
    Res = case snarl_user:auth(UserB, ensure_binary(Pass)) of
              {ok, not_found} ->
                  {error, not_found};
              {ok, Obj}  ->
                  {ok, UUID} = jsxd:get(<<"uuid">>, Obj),
                  {ok, Token} = snarl_token:add(UUID),
                  {ok, {token, Token}}
          end,
    {stop, normal,
     Res,
     State};

message({user, allowed, {token, Token}, Permission}, State) ->
    case snarl_token:get(Token) of
        {ok, not_found} ->
            {stop, normal, false, State};
        {ok, User} ->
            {stop, normal,
             snarl_user:allowed(User, Permission),
             State}
    end;

message({user, allowed, User, Permission}, State) ->
    {stop, normal,
     snarl_user:allowed(ensure_binary(User), Permission),
     State};

message({user, delete, User}, State) ->
    {stop, normal,
     snarl_user:delete(ensure_binary(User)),
     State};

message({user, passwd, User, Pass}, State) ->
    {stop, normal,
     snarl_user:passwd(ensure_binary(User), ensure_binary(Pass)),
     State};

message({user, join, User, Group}, State) ->
    {stop, normal, snarl_user:join(ensure_binary(User), Group), State};

message({user, leave, User, Group}, State) ->
    {stop, normal, snarl_user:leave(ensure_binary(User), Group), State};

message({user, grant, User, Permission}, State) ->
    {stop, normal, snarl_user:grant(ensure_binary(User), Permission), State};

message({user, revoke, User, Permission}, State) ->
    {stop, normal, snarl_user:revoke(ensure_binary(User), Permission), State};

message({user, revoke_all, User, Permission}, State) ->
    {stop, normal, snarl_user:revoke_all(ensure_binary(User), Permission), State};

message({token, delete, Token}, State) when
      is_binary(Token) ->
    {stop, normal, snarl_token:delete(Token), State};


%%%===================================================================
%%% Resource Functions
%%%===================================================================

message({user, set_resource, User, Resource, Value}, State) ->
    {stop, normal, snarl_user:set_resource(ensure_binary(User), Resource, Value), State};

                                                %message({user, get_resource, User, Resource}, State) ->
                                                %    {stop, normal, snarl_user:get_resource(ensure_binary(User), Resource), State};

message({user, claim_resource, User, Resource, Ammount}, State) ->
    ID = uuid:uuid4(),
    {stop, normal, {ID, snarl_user:claim_resource(ensure_binary(User), ID, Resource, Ammount)}, State};

message({user, free_resource, User, Resource, ID}, State) ->
    {stop, normal, snarl_user:free_resource(ensure_binary(User), Resource, ID), State};

message({user, resource_stat, User}, State) ->
    {stop, normal, snarl_user:get_resource_stat(ensure_binary(User)), State};

%%%===================================================================
%%% Group Functions
%%%===================================================================

message({group, list}, State) ->
    {stop, normal, snarl_group:list(), State};

message({group, get, Group}, State) ->
    {stop, normal, snarl_group:get(Group), State};

message({group, set, Group, Attribute, Value}, State) when
      is_binary(Group) ->
    {stop, normal,
     snarl_group:set(Group, Attribute, Value),
     State};

message({group, set, Group, Attributes}, State) when
      is_binary(Group) ->
    {stop, normal,
     snarl_group:set(Group, Attributes),
     State};

message({group, add, Group}, State) ->
    {stop, normal, snarl_group:add(Group), State};

message({group, delete, Group}, State) ->
    {stop, normal, snarl_group:delete(Group), State};

message({group, grant, Group, Permission}, State) when
      is_binary(Group),
      is_list(Permission)->
    {stop, normal, snarl_group:grant(Group, Permission), State};

message({group, revoke, Group, Permission}, State) ->
    {stop, normal, snarl_group:revoke(Group, Permission), State};

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
