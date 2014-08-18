-module(snarl_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, init_folsom/0]).

-ignore_xref([init_folsom/0]).

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
    init_folsom(),
    case snarl_sup:start_link() of
        {ok, Pid} ->
            ?SRV_WITH_AAE(snarl_user_vnode, snarl_user),
            ?SRV_WITH_AAE(snarl_role_vnode, snarl_role),
            ?SRV_WITH_AAE(snarl_org_vnode, snarl_org),

            ok = riak_core:register([{vnode_module, snarl_token_vnode}]),
            ok = riak_core_node_watcher:service_up(snarl_token, self()),

            ok = riak_core_ring_events:add_guarded_handler(snarl_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(snarl_node_event_handler, []),

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
            timer:apply_after(2000, snarl_opt, update, []),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.


init_folsom() ->
    [folsom_metrics:new_histogram(Name, slide, 60) ||
        Name <-
            [
             {fifo_db, fold_keys},
             {fifo_db, fold},
             {fifo_db, get},
             {fifo_db, put},
             {fifo_db, delete},
             {fifo_db, transact}
            ]].
