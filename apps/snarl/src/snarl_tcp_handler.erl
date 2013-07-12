-module(snarl_tcp_handler).

-include("snarl.hrl").

-include("snarl_version.hrl").

-export([init/2, message/2]).

-ignore_xref([init/2, message/2]).

-record(state, {port}).

init(Prot, []) ->
    {ok, #state{port = Prot}}.

%%%===================================================================
%%% General Functions
%%%===================================================================

-spec message(fifo:snarl_message(), term()) -> any().

message(version, State) ->
    {reply, {ok, ?VERSION}, State};

%%%===================================================================
%%% Org Functions
%%%===================================================================

message({org, list}, State) ->
    {reply, snarl_org:list(), State};

message({org, get, Org}, State) ->
    {reply, snarl_org:get(Org), State};

message({org, set, Org, Attribute, Value}, State) when
      is_binary(Org) ->
    {reply,
     snarl_org:set(Org, Attribute, Value),
     State};

message({org, set, Org, Attributes}, State) when
      is_binary(Org) ->
    {reply,
     snarl_org:set(Org, Attributes),
     State};

message({org, add, Org}, State) ->
    {reply, snarl_org:add(Org), State};

message({org, delete, Org}, State) ->
    {reply, snarl_org:delete(Org), State};

message({org, trigger, add, Org, Trigger}, State) ->
    {reply, snarl_org:add_trigger(Org, Trigger), State};

message({org, trigger, remove, Org, Trigger}, State) ->
    {reply, snarl_org:remove_trigger(Org, Trigger), State};

message({org, trigger, execute, Org, Event, Payload}, State) ->
    {reply, snarl_org:trigger(Org, Event, Payload), State};

%%%===================================================================
%%% User Functions
%%%===================================================================


message({user, list}, State) ->
    {reply, snarl_user:list(), State};

message({user, get, {token, Token}}, State) ->
    case snarl_token:get(Token) of
        {ok, not_found} ->
            {reply, not_found, State};
        {ok, User} ->
            message({user, get, User}, State)
    end;

message({user, get, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:get(User),
     State};

message({user, keys, find, KeyID}, State) when
      is_binary(KeyID) ->
    {reply,
     snarl_user:find_key(KeyID),
     State};

message({user, keys, get, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:keys(User),
     State};

message({user, keys, add, User, KeyId, Key}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:add_key(User, KeyId, Key),
     State};

message({user, keys, revoke, User, KeyId}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:revoke_key(User, KeyId),
     State};

message({user, set, User, Attribute, Value}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:set(User, Attribute, Value),
     State};

message({user, set, User, Attributes}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:set(User, Attributes),
     State};

message({user, lookup, User}, State) when is_binary(User) ->
    {reply,
     snarl_user:lookup(User),
     State};

message({user, cache, {token, Token}}, State) ->
    case snarl_token:get(Token) of
        {ok, not_found} ->
            {reply, not_found, State};
        {ok, User} ->
            message({user, cache, User}, State)
    end;

message({user, cache, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:cache(User),
     State};

message({user, add, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:add(User),
     State};

message({user, auth, User, Pass}, State) when
      is_binary(User),
      is_binary(Pass) ->
    Res = case snarl_user:auth(User, Pass) of
              not_found ->
                  {error, not_found};
              {ok, UUID}  ->
                  {ok, Token} = snarl_token:add(UUID),
                  {ok, {token, Token}}
          end,
    {reply,
     Res,
     State};

message({user, allowed, {token, Token}, Permission}, State) ->
    case snarl_token:get(Token) of
        {ok, not_found} ->
            {reply, false, State};
        {ok, User} ->
            {reply,
             snarl_user:allowed(User, Permission),
             State}
    end;

message({user, allowed, User, Permission}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:allowed(User, Permission),
     State};

message({user, delete, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:delete(User),
     State};

message({user, passwd, User, Pass}, State) when
      is_binary(User),
      is_binary(Pass) ->
    {reply,
     snarl_user:passwd(User, Pass),
     State};

message({user, join, User, Group}, State) when
      is_binary(User),
      is_binary(Group) ->
    {reply, snarl_user:join(User, Group), State};

message({user, leave, User, Group}, State) when
      is_binary(User),
      is_binary(Group) ->
    {reply, snarl_user:leave(User, Group), State};

message({user, grant, User, Permission}, State) when
      is_binary(User) ->
    {reply, snarl_user:grant(User, Permission), State};

message({user, revoke, User, Permission}, State) when
      is_binary(User) ->
    {reply, snarl_user:revoke(User, Permission), State};

message({user, revoke_prefix, User, Prefix}, State) when
      is_binary(User) ->
    {reply, snarl_user:revoke_prefix(User, Prefix), State};

message({token, delete, Token}, State) when
      is_binary(Token) ->
    {reply, snarl_token:delete(Token), State};

message({user, org, get, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:orgs(User),
     State};

message({user, org, active, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:active(User),
     State};

message({user, org, join, User, Org}, State) when
      is_binary(User),
      is_binary(Org) ->
    {reply, snarl_user:join_org(User, Org), State};

message({user, org, leave, User, Org}, State) when
      is_binary(User),
      is_binary(Org) ->
    {reply, snarl_user:leave_org(User, Org), State};

message({user, org, select, User, Org}, State) when
      is_binary(User),
      is_binary(Org) ->
    {reply, snarl_user:select_org(User, Org), State};

%%%===================================================================
%%% Group Functions
%%%===================================================================

message({group, list}, State) ->
    {reply, snarl_group:list(), State};

message({group, get, Group}, State) ->
    {reply, snarl_group:get(Group), State};

message({group, set, Group, Attribute, Value}, State) when
      is_binary(Group) ->
    {reply,
     snarl_group:set(Group, Attribute, Value),
     State};

message({group, set, Group, Attributes}, State) when
      is_binary(Group) ->
    {reply,
     snarl_group:set(Group, Attributes),
     State};

message({group, add, Group}, State) ->
    {reply, snarl_group:add(Group), State};

message({group, delete, Group}, State) ->
    {reply, snarl_group:delete(Group), State};

message({group, grant, Group, Permission}, State) when
      is_binary(Group),
      is_list(Permission)->
    {reply, snarl_group:grant(Group, Permission), State};

message({group, revoke, Group, Permission}, State) ->
    {reply, snarl_group:revoke(Group, Permission), State};

message({group, revoke_prefix, Group, Prefix}, State) ->
    {reply, snarl_group:revoke_prefix(Group, Prefix), State};

message(Message, State) ->
    io:format("Unsuppored TCP message: ~p", [Message]),
    {noreply, State}.
