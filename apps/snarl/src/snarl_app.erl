-module(snarl_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(SRV(VNode, Srv),
        ok = riak_core:register([{vnode_module, VNode}]),
        ok = riak_core_node_watcher:service_up(Srv, self())).

-define(SRV_WITH_AAE(VNode, Srv),
        ?SRV(VNode, Srv),
        ok = riak_core_capability:register({Srv, anti_entropy},
                                           [enabled_v1, disabled],
                                           enabled_v1)).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case application:get_env(fifo_db, db_path) of
        {ok, _} ->
            ok;
        undefined ->
            case application:get_env(snarl, db_path) of
                {ok, P} ->
                    application:set_env(fifo_db, db_path, P);
                _ ->
                    application:set_env(fifo_db, db_path, "/var/db/snarl")
            end
    end,
    case application:get_env(fifo_db, backend) of
        {ok, _} ->
            ok;
        undefined ->
            application:set_env(fifo_db, backend, fifo_db_hanoidb)
    end,
    case snarl_sup:start_link() of
        {ok, Pid} ->
            ?SRV_WITH_AAE(snarl_user_vnode, snarl_user),
            ?SRV_WITH_AAE(snarl_group_vnode, snarl_group),
            ?SRV_WITH_AAE(snarl_org_vnode, snarl_org),

            ok = riak_core:register([{vnode_module, snarl_token_vnode}]),
            ok = riak_core_node_watcher:service_up(snarl_token, self()),

            ok = riak_core_ring_events:add_guarded_handler(snarl_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(snarl_node_event_handler, []),

            statman_server:add_subscriber(statman_aggregator),
            snarl_snmp_handler:start(),
            case application:get_env(snarl, sync) of
                {ok, on} ->
                    {ok, {IP, Port}} = application:get_env(snarl, sync_ip),
                    [A, B, C, D] = [list_to_integer(binary_to_list(P)) || P <- re:split(IP, "[.]")],
                    {ok, _} = ranch:start_listener(
                                snarl, 100, ranch_tcp,
                                [{port, Port}, {ip, {A,B,C,D}}],
                                snarl_sync_protocol, []);
                _ ->
                    ok
            end,

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
