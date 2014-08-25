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

-type message() ::
        fifo:snarl_message() |
        fifo:snarl_user_message() |
        fifo:snarl_org_message() |
        fifo:snarl_role_message() |
        fifo:snarl_token_message().

-spec message(message(), #state{}) ->
                     {noreply, #state{}} |
                     {reply, term(), #state{}}.

message(version, State) ->
    {reply, {ok, ?VERSION}, State};

%%%===================================================================
%%% Org Functions
%%%===================================================================

message({org, list, Realm}, State) ->
    {reply, snarl_org:list(Realm), State};

message({org, list, Realm, Requirements, Full}, State) ->
    {reply, snarl_org:list(Realm, Requirements, Full), State};

message({org, get, Realm, Org}, State) ->
    {reply, snarl_org:get(Realm, Org), State};

message({org, set_metadata, Realm, Org, Attributes}, State) when
      is_binary(Org) ->
    {reply,
     snarl_org:set_metadata(Realm, Org, Attributes),
     State};

message({org, add, Realm, Org}, State) ->
    {reply, snarl_org:add(Realm, Org), State};

message({org, delete, Realm, Org}, State) ->
    {reply, snarl_org:delete(Realm, Org), State};

message({org, trigger, add, Realm, Org, Trigger}, State) ->
    {reply, snarl_org:add_trigger(Realm, Org, Trigger), State};

message({org, trigger, remove, Realm, Org, Trigger}, State) ->
    {reply, snarl_org:remove_trigger(Realm, Org, Trigger), State};

message({org, trigger, execute, Realm, Org, Event, Payload}, State) ->
    {reply, snarl_org:trigger(Realm, Org, Event, Payload), State};

%%%===================================================================
%%% User Functions
%%%===================================================================

message({user, list, Realm}, State) ->
    {reply, snarl_user:list(Realm), State};

message({user, list, Realm, Requirements, Full}, State) ->
    {reply, snarl_user:list(Realm, Requirements, Full), State};

message({user, get, Realm, {token, Token}}, State) ->
    case snarl_token:get(Realm, Token) of
        not_found ->
            {reply, not_found, State};
        {ok, User} ->
            message({user, get, Realm, User}, State)
    end;

message({user, get, Realm, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:get(Realm, User),
     State};

message({user, keys, find, Realm, KeyID}, State) when
      is_binary(KeyID) ->
    {reply,
     snarl_user:find_key(Realm, KeyID),
     State};

message({user, keys, get, Realm, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:keys(Realm, User),
     State};

message({user, keys, add, Realm, User, KeyId, Key}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:add_key(Realm, User, KeyId, Key),
     State};

message({user, keys, revoke, Realm, User, KeyId}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:revoke_key(Realm, User, KeyId),
     State};


message({user, yubikeys, get, Realm, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:yubikeys(Realm, User),
     State};

message({user, yubikeys, add, Realm, User, OTP}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:add_yubikey(Realm, User, OTP),
     State};

message({user, yubikeys, remove, Realm, User, KeyId}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:remove_yubikey(Realm, User, KeyId),
     State};

message({user, set_metadata, Realm, User, Attributes}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:set_metadata(Realm, User, Attributes),
     State};

message({user, lookup, Realm, User}, State) when is_binary(User) ->
    {reply,
     snarl_user:lookup(Realm, User),
     State};

message({user, cache, Realm, {token, Token}}, State = #state{}) ->
    case snarl_token:get(Realm, Token) of
        not_found ->
            {reply, not_found, State};
        {ok, User} ->
            message({user, cache, Realm, User}, State)
    end;

message({user, cache, Realm, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:cache(Realm, User),
     State};

message({user, add, Realm, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:add(Realm, User),
     State};

message({user, add, Realm, Creator, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:add(Realm, Creator, User),
     State};

message({user, auth, Realm, User, Pass}, State) when
      is_binary(User),
      is_binary(Pass) ->
    message({user, auth, Realm, User, Pass, <<>>}, State);

message({user, auth, Realm, User, Pass, basic}, State) when
      is_binary(User),
      is_binary(Pass) ->
    Res = case snarl_user:auth(Realm, User, Pass, basic) of
              not_found ->
                  {error, not_found};
              {ok, UUID}  ->
                  {ok, UUID}
          end,
    {reply,
     Res,
     State};

message({user, auth, Realm, User, Pass, OTP}, State) when
      is_binary(User),
      is_binary(Pass),
      is_binary(OTP) ->
    Res = case snarl_user:auth(Realm, User, Pass, OTP) of
              not_found ->
                  {error, not_found};
              {ok, UUID}  ->
                  {ok, Token} = snarl_token:add(Realm, UUID),
                  {ok, {token, Token}}
          end,
    {reply,
     Res,
     State};

message({user, allowed, Realm, {token, Token}, Permission}, State) ->
    case snarl_token:get(Realm, Token) of
        not_found ->
            {reply, false, State};
        {ok, User} ->
            {reply,
             snarl_user:allowed(Realm, User, Permission),
             State}
    end;

message({user, allowed, Realm, User, Permission}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:allowed(Realm, User, Permission),
     State};

message({user, delete, Realm, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:delete(Realm, User),
     State};

message({user, passwd, Realm, User, Pass}, State) when
      is_binary(User),
      is_binary(Pass) ->
    {reply,
     snarl_user:passwd(Realm, User, Pass),
     State};

message({user, join, Realm, User, Role}, State) when
      is_binary(User),
      is_binary(Role) ->
    {reply, snarl_user:join(Realm, User, Role), State};

message({user, leave, Realm, User, Role}, State) when
      is_binary(User),
      is_binary(Role) ->
    {reply, snarl_user:leave(Realm, User, Role), State};

message({user, grant, Realm, User, Permission}, State) when
      is_binary(User) ->
    {reply, snarl_user:grant(Realm, User, Permission), State};

message({user, revoke, Realm, User, Permission}, State) when
      is_binary(User) ->
    {reply, snarl_user:revoke(Realm, User, Permission), State};

message({user, revoke_prefix, Realm, User, Prefix}, State) when
      is_binary(User) ->
    {reply, snarl_user:revoke_prefix(Realm, User, Prefix), State};

message({token, delete, Realm, Token}, State) when
      is_binary(Token) ->
    {reply, snarl_token:delete(Realm, Token), State};

message({user, org, get, Realm, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:orgs(Realm, User),
     State};

message({user, org, active, Realm, User}, State) when
      is_binary(User) ->
    {reply,
     snarl_user:active(Realm, User),
     State};

message({user, org, join, Realm, User, Org}, State) when
      is_binary(User),
      is_binary(Org) ->
    {reply, snarl_user:join_org(Realm, User, Org), State};

message({user, org, leave, Realm, User, Org}, State) when
      is_binary(User),
      is_binary(Org) ->
    {reply, snarl_user:leave_org(Realm, User, Org), State};

message({user, org, select, Realm, User, Org}, State) when
      is_binary(User),
      is_binary(Org) ->
    {reply, snarl_user:select_org(Realm, User, Org), State};

%%%===================================================================
%%% Role Functions
%%%===================================================================

message({role, list, Realm}, State) ->
    {reply, snarl_role:list(Realm), State};

message({role, list, Realm, Requirements, Full}, State) ->
    {reply, snarl_role:list(Realm, Requirements, Full), State};

message({role, get, Realm, Role}, State) ->
    {reply, snarl_role:get(Realm, Role), State};

message({role, set_metadata, Realm, Role, Attributes}, State) when
      is_binary(Role) ->
    {reply,
     snarl_role:set_metadata(Realm, Role, Attributes),
     State};

message({role, add, Realm, Role}, State) ->
    {reply, snarl_role:add(Realm, Role), State};

message({role, delete, Realm, Role}, State) ->
    {reply, snarl_role:delete(Realm, Role), State};

message({role, grant, Realm, Role, Permission}, State) when
      is_binary(Role),
      is_list(Permission)->
    {reply, snarl_role:grant(Realm, Role, Permission), State};

message({role, revoke, Realm, Role, Permission}, State) ->
    {reply, snarl_role:revoke(Realm, Role, Permission), State};

message({role, revoke_prefix, Realm, Role, Prefix}, State) ->
    {reply, snarl_role:revoke_prefix(Realm, Role, Prefix), State};

message({cloud, status, Realm}, State) ->
    {reply,
     status(Realm),
     State};

message(Message, State) ->
    lager:warning("Unsuppored TCP message: ~p", [Message]),
    {noreply, State}.

status(Realm) ->
    {Us, Gs, Os} = case application:get_env(snarl, status_include_count) of
                       {ok, ture} ->
                           {ok, UsX} = snarl_user:list(Realm),
                           {ok, GsX} = snarl_role:list(Realm),
                           {ok, OsX} = snarl_org:list(Realm),
                           {UsX, GsX, OsX};
                       _ ->
                           {[], [], []}
                   end,
    Resources = [{<<"users">>, length(Us)},
                 {<<"roles">>, length(Gs)},
                 {<<"orgs">>, length(Os)}],
    Warnings = case riak_core_status:transfers() of
                   {[], []} ->
                       [];
                   {[], L} ->
                       W = jsxd:from_list(
                             [{<<"category">>, <<"snarl">>},
                              {<<"element">>, <<"handoff">>},
                              {<<"type">>, <<"info">>},
                              {<<"message">>, bin_fmt("~b handofs pending.",
                                                      [length(L)])}]),
                       [W];
                   {S, []} ->
                       server_errors(S);
                   {S, L} ->
                       W = jsxd:from_list(
                             [{<<"category">>, <<"snarl">>},
                              {<<"element">>, <<"handoff">>},
                              {<<"type">>, <<"info">>},
                              {<<"message">>, bin_fmt("~b handofs pending.",
                                                      [length(L)])}]),
                       [W | server_errors(S)]
               end,
    {ok, {ordsets:from_list(Resources), ordsets:from_list(Warnings)}}.


server_errors(Servers) ->
    lists:map(fun (Server) ->
                      jsxd:from_list(
                        [{<<"category">>, <<"snarl">>},
                         {<<"element">>, list_to_binary(atom_to_list(Server))},
                         {<<"type">>, <<"critical">>},
                         {<<"message">>, bin_fmt("Snarl server ~s down.", [Server])}])
              end, Servers).

bin_fmt(F, L) ->
    list_to_binary(io_lib:format(F, L)).
