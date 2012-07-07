-module(snarl_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, load/0]).

load() ->
    application:start(sasl),
    application:start(lager),
    application:start(crypto),
    application:start(nodefinder),
    application:start(backyard),
    application:start(redo),
    application:start(uuid),
    application:start(snarl).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    snarl_sup:start_link().

stop(_State) ->
    ok.
