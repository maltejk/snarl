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
            ok = riak_core:register([{vnode_module, snarl_user_vnode}]),
            ok = riak_core_node_watcher:service_up(snarl_user, self()),

            ok = riak_core:register([{vnode_module, snarl_group_vnode}]),
            ok = riak_core_node_watcher:service_up(snarl_group, self()),

            ok = riak_core:register([{vnode_module, snarl_token_vnode}]),
            ok = riak_core_node_watcher:service_up(snarl_token, self()),

            ok = riak_core:register([{vnode_module, snarl_org_vnode}]),
            ok = riak_core_node_watcher:service_up(snarl_org, self()),

            ok = riak_core_ring_events:add_guarded_handler(snarl_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(snarl_node_event_handler, []),

            statman_server:add_subscriber(statman_aggregator),
            snarl_snmp_handler:start(),
            case application:get_env(newrelic,license_key) of
                undefined ->
                    ok;
                _ ->
                    newrelic_poller:start_link(fun newrelic_statman:poll/0)
            end,

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
