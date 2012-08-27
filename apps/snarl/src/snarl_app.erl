-module(snarl_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case snarl_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, snarl_vnode}]),
	    ok = riak_core_node_watcher:service_up(snarl, self()),

            ok = riak_core:register([{vnode_module, snarl_user_vnode}]),
	    ok = riak_core_node_watcher:service_up(snarl_user, self()),

            ok = riak_core:register([{vnode_module, snarl_group_vnode}]),
	    ok = riak_core_node_watcher:service_up(snarl_group, self()),

            ok = riak_core:register([{vnode_module, snarl_permissions_vnode}]),
	    ok = riak_core_node_watcher:service_up(snarl_permissions, self()),

            ok = riak_core_ring_events:add_guarded_handler(snarl_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(snarl_node_event_handler, []),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
