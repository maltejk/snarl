-module(snarl_oauth_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    application:set_env(oauth2, backend, snarl_oauth_backend),
    snarl_oauth_sup:start_link().

stop(_State) ->
    ok.
