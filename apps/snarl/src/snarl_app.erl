-module(snarl_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, load/0]).

check_grid() ->
    timer:sleep(100),
    case length(redgrid:nodes()) of
	1 ->
	    check_grid();
	_ ->
	    ok
    end.

load() ->
    application:start(sasl),
    application:start(alog),
    application:start(redgrid),
    application:start(crypto),
    application:start(redo),
    application:start(uuid),
    application:start(gproc),
    application:start(snarl).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    check_grid(),
    snarl_sup:start_link().

stop(_State) ->
    ok.
