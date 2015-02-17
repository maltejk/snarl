-module(snarl_oauth_backend).
-behaviour(oauth2_backend).

-include("snarl_oauth.hrl").

%%% API
%% -export([add_client/4,
%%          add_resowner/2,
%%          add_resowner/3,
%%          delete_client/1,
%%          delete_resowner/1,
%%          create/0,
%%          delete/0
%%         ]).

%%% OAuth2 backend functionality
-export([authenticate_user/2,
         authenticate_client/2,
         associate_access_code/3,
         associate_access_token/3,
         associate_refresh_token/3,
         resolve_access_code/2,
         resolve_access_token/2,
         resolve_refresh_token/2,
         revoke_access_code/2,
         revoke_access_token/2,
         revoke_refresh_token/2,
         get_client_identity/2,
         verify_redirection_uri/3,
         verify_client_scope/3,
         verify_resowner_scope/3,
         verify_scope/3
        ]).

-define(ACCESS_CODE_TABLE, access_codes).
-define(ACCESS_TOKEN_TABLE, access_tokens).
-define(REFRESH_TOKEN_TABLE, refresh_tokens).
-define(USER_TABLE, users).
-define(CLIENT_TABLE, clients).
-define(REQUEST_TABLE, requests).


-define(TABLES, [?ACCESS_CODE_TABLE,
                 ?ACCESS_TOKEN_TABLE,
                 ?REFRESH_TOKEN_TABLE,
                 ?USER_TABLE,
                 ?CLIENT_TABLE,
                 ?REQUEST_TABLE]).

%% -record(client, {
%%           client_id     :: binary(),
%%           client_secret :: binary(),
%%           redirect_uri  :: binary(),
%%           scope         :: [binary()]
%%          }).

%%-type client() :: #client{}.

%% -record(resowner, {
%%           username  :: binary(),
%%           password  :: binary(),
%%           scope     :: scope()
%%          }).

%%-type grantctx() :: oauth2:context().
%%-type appctx()   :: oauth2:appctx().
%%-type token()    :: oauth2:token().
%%-type scope()    :: oauth2:scope().

%%%===================================================================
%%% OAuth2 backend functions
%%%===================================================================
authenticate_user({UserID}, AppContext) ->
    {ok, {AppContext, UserID}};

authenticate_user({Username, Password}, AppContext) ->
    authenticate_user({Username, Password, <<>>}, AppContext);

authenticate_user({Username, Password, OTP}, AppContext) ->

    %%case get(?USER_TABLE, Username) of
    case snarl_user:auth(AppContext#oauth_state.realm, Username, Password, OTP) of
        {ok, UserID} -> %#resowner{password = Password} = Identity} ->
            {ok, {AppContext, UserID}};
        %%{ok, #resowner{password = _WrongPassword}} ->
        %%    {error, badpass};
        not_found ->
            {error, notfound}
    end.

authenticate_client({UserID}, AppContext) ->
    {ok, {AppContext, UserID}};
authenticate_client({ClientId, ClientSecret}, AppContext) ->
    case snarl_user:auth(AppContext#oauth_state.realm, <<"client:", ClientId/binary>>, ClientSecret, <<>>) of
        %%case get(?CLIENT_TABLE, ClientId) of
        {ok, UserID} -> %#resowner{password = Password} = Identity} ->
            {ok, {AppContext, UserID}};
        %%{ok, #resowner{password = _WrongPassword}} ->
        %%    {error, badpass};
        not_found ->
            {error, notfound}
    end.

%% Is this a Authrorization Code?
associate_access_code(AccessCode, Context, AppContext) ->
    %% put(?ACCESS_CODE_TABLE, AccessCode, Context),
    snarl_token:add(AppContext#oauth_state.realm,
                    {?ACCESS_CODE_TABLE, AccessCode},
                    oauth2_config:expiry_time(code_grant),
                    Context),
    {ok, AppContext}.

resolve_access_code(AccessCode, AppContext) ->
    %% case get(?ACCESS_CODE_TABLE, AccessCode) of
    case snarl_token:get(AppContext#oauth_state.realm,{?ACCESS_CODE_TABLE, AccessCode}) of
        {ok, Context} -> %% Was Grant
            {ok, {AppContext, Context}};
        not_found ->
            {error, notfound}
    end.

%% @doc Revokes an access code AccessCode, so that it cannot be used again.
revoke_access_code(AccessCode, AppContext) ->
    snarl_token:delete(AppContext#oauth_state.realm, {?ACCESS_CODE_TABLE, AccessCode}),
        {ok, AppContext}.

associate_access_token(AccessToken, Context, AppContext) ->
    snarl_token:add(AppContext#oauth_state.realm,
                    {?ACCESS_TOKEN_TABLE, AccessToken},
                    oauth2_config:expiry_time(expiery_time), %% TODO: is this a grant
                    Context),
    {ok, AppContext}.

resolve_access_token(AccessToken, AppContext) ->
    %% case get(?ACCESS_TOKEN_TABLE, AccessToken) of
    case snarl_token:get(AppContext#oauth_state.realm, {?ACCESS_TOKEN_TABLE, AccessToken}) of
        {ok, Context} -> %% Was Grant
            {ok, {AppContext, Context}};
        not_found ->
            {error, notfound}
    end.

%% Not implemented yet.
revoke_access_token(AccessToken, AppContext) ->
    snarl_token:delete(AppContext#oauth_state.realm, {?ACCESS_TOKEN_TABLE, AccessToken}),
    {ok, AppContext}.


%% Refresh Tokens are handed out linking to a resource owner
%% with the maximum available permissions.
%%
%% It can be used to get a new access token.
associate_refresh_token(RefreshToken, Context, AppContext) ->
    snarl_token:add(AppContext#oauth_state.realm,
                    {?REFRESH_TOKEN_TABLE, RefreshToken},
                    oauth2_config:expiry_time(refresh_token),
                    Context),
    %% put(?REFRESH_TOKEN_TABLE, RefreshToken, Context),
    {ok, AppContext}.

resolve_refresh_token(RefreshToken, AppContext) ->
    %% case get(?REFRESH_TOKEN_TABLE, RefreshToken) of
    case snarl_token:get(AppContext#oauth_state.realm, {?REFRESH_TOKEN_TABLE, RefreshToken}) of
        {ok, Context} -> %% Was Grant
            {ok, {AppContext, Context}};
        not_found ->
            {error, notfound}
    end.

revoke_refresh_token(RefreshToken, AppContext) ->
    snarl_token:delete(AppContext#oauth_state.realm, {?REFRESH_TOKEN_TABLE, RefreshToken}),
    {ok, AppContext}.

get_client_identity(ClientId, AppContext) ->
    case snarl_user:lookup(AppContext#oauth_state.realm, <<"client:", ClientId/binary>>) of
        {ok, Client} ->
            {ok, {AppContext, ft_user:uuid(Client)}};
        not_found ->
            {error, notfound}
    end.

verify_redirection_uri(_Client, undefined, AppContext) ->
    {ok, AppContext};
verify_redirection_uri(_Client, <<>>, AppContext) ->
    {ok, AppContext};
verify_redirection_uri(ClientUUID, Uri, AppContext) ->
    {ok, Client} = snarl_user:get(AppContext#oauth_state.realm, ClientUUID),
    Metadata = ft_user:metadata(Client),
    case jsxd:get([<<"oauth2">>, <<"redirection_uri">>], Metadata) of
        {ok, <<>>} ->
            {error, baduri};
        {ok, Uri} ->
            {ok, AppContext};
        _ ->
            {error, baduri}
    end.

verify_client_scope(_Client, Scope, AppContext) ->
    %% TODO: Do we need to look at what the scope of the client should be?
    RealmScope = [S || {S, _, _} <-
                           snarl_oauth:scope(AppContext#oauth_state.realm)],
    verify_scope(RealmScope, Scope, AppContext).
%%verify_client_scope({Client, _Secret}, Scope, AppContext) ->
    %% case snarl_user:lookup(AppContext#oauth_state.realm, <<"client:", Client/binary>>) of
    %%     {ok, ClientID} ->

    %%     _E ->
    %%         {error, badscope}
    %% end.

verify_resowner_scope(_UserID, Scope, AppContext) ->
    RealmScope = [S || {S, _, _} <-
                           snarl_oauth:scope(AppContext#oauth_state.realm)],
    verify_scope(RealmScope, Scope, AppContext).

%% verify_resowner_scope(UserID, Scope, AppContext) ->
%%     {ok, Perms} = snarl_user:cache(AppContext#oauth_state.realm, UserID),
%%     verify_scope([permissions_to_scope(E) || E <- Perms], Scope, AppContext).

verify_scope(RegisteredScope, undefined, AppContext) ->
    {ok, {AppContext, RegisteredScope}};
verify_scope(_RegisteredScope, [], AppContext) ->
    {ok, {AppContext, []}};
verify_scope([], _Scope, _AppContext) ->
    {error, invalid_scope};
verify_scope(RegisteredScope, Scope, AppContext) ->
    case oauth2_priv_set:is_subset(oauth2_priv_set:new(Scope),
                                   oauth2_priv_set:new(RegisteredScope)) of
        true ->
            {ok, {AppContext, Scope}};
        false ->
            {error, badscope}
    end.

%% permissions_to_scope([])->
%%     <<>>;
%% permissions_to_scope(Perm)->
%%     <<".", Res/binary>> = permissions_to_scope1(Perm),
%%     Res.

%% permissions_to_scope1([<<"...">>| _]) ->
%%     <<".*">>;
%% permissions_to_scope1([<<"_">>| _]) ->
%%     <<".*">>;
%% permissions_to_scope1([<<>>]) ->
%%     <<>>;
%% permissions_to_scope1([]) ->
%%     <<>>;
%% permissions_to_scope1([E| R]) ->
%%     <<".", E/binary, (permissions_to_scope1(R))/binary>>.

%%%===================================================================
%%% API
%%%===================================================================

%% -spec add_client(Id, Secret, RedirectURI, Scope) -> ok when
%%       Id          :: binary(),
%%       Secret      :: binary(),
%%       RedirectURI :: binary(),
%%       Scope       :: [binary()].
%% add_client(Id, Secret, RedirectURI, Scope) ->
%%     put(?CLIENT_TABLE, Id, #client{client_id = Id,
%%                                    client_secret = Secret,
%%                                    redirect_uri = RedirectURI,
%%                                    scope = Scope
%%                                   }),
%%     ok.

%% -spec add_resowner(Username, Password) -> ok when
%%       Username :: binary(),
%%       Password :: binary().
%% add_resowner(Username, Password) ->
%%     add_resowner(Username, Password, []),
%%     ok.

%% -spec add_resowner(Username, Password, Scope) -> ok when
%%       Username  :: binary(),
%%       Password  :: binary(),
%%       Scope     :: [binary()].
%% add_resowner(Username, Password, Scope) ->
%%     put(?USER_TABLE, Username, #resowner{username = Username,
%%                                          password = Password, scope = Scope}),
%%     ok.


%% -spec create() -> ok.
%% create() ->
%%     lists:foreach(fun(Table) ->
%%                           ets:new(Table, [named_table, public])
%%                   end,
%%                   ?TABLES),
%%     ok.

%% -spec delete() -> ok.
%% delete() ->
%%     lists:foreach(fun ets:delete/1, ?TABLES),
%%     ok.

%% -spec delete_resowner(Username) -> ok when
%%       Username :: binary().
%% delete_resowner(Username) ->
%%     delete(?USER_TABLE, Username),
%%     ok.

%% -spec delete_client(Id) -> ok when
%%       Id :: binary().
%% delete_client(Id) ->
%%     delete(?CLIENT_TABLE, Id),
%%     ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%get(Table, Key) ->
%%    case ets:lookup(Table, Key) of
%%        [] ->
%%            {error, notfound};
%%        [{_Key, Value}] ->
%%            {ok, Value}
%%    end.

%%put(Table, Key, Value) ->
%%    ets:insert(Table, {Key, Value}).

%%delete(Table, Key) ->
%%    ets:delete(Table, Key).
