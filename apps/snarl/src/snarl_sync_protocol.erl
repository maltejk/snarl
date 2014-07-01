-module(snarl_sync_protocol).

-behaviour(gen_server).
-behaviour(ranch_protocol).

-export([start_link/4,
         init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ignore_xref([start_link/4]).

-record(state, {socket, transport, ok, error, closed}).

start_link(ListenerPid, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [[ListenerPid, Socket, Transport, Opts]]).

init([ListenerPid, Socket, Transport, _Opts = []]) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(ListenerPid),
    ok = Transport:setopts(Socket, [{active, true}, {packet,4}]),
    {OK, Closed, Error} = Transport:messages(),
    gen_server:enter_loop(?MODULE, [], #state{
                                          ok = OK,
                                          closed = Closed,
                                          error = Error,
                                          socket = Socket,
                                          transport = Transport}).

handle_info({_OK, Socket, BinData}, State = #state{
                                               transport = Transport,
                                               ok = _OK}) ->
    case binary_to_term(BinData) of
        ping ->
            Transport:send(Socket, term_to_binary(pong)),
            {noreply, State};
        get_tree ->
            {ok, Tree} = snarl_sync_tree:get_tree(),
            Transport:send(Socket, term_to_binary({ok, Tree})),
            {noreply, State};
        {raw, System, UUID} ->
            Data = System:raw(UUID),
            Transport:send(Socket, term_to_binary(Data)),
            {noreply, State};
        {repair, System, UUID, Obj} ->
            System:sync_repair(UUID, Obj),
            {noreply, State};
        {delete, System, UUID} ->
            System:delete(UUID),
            {noreply, State};
        {write, Node, VNode, System, Bucket, Entity, Op, Val} ->
            NVS = {{remote, Node}, VNode, System, Bucket},
            snarl_entity_write_fsm:write(NVS, Entity, Op, Val),
            {noreply, State}
    end;

handle_info(Info, State) ->
    lager:warning("[mdns server] Unknown message: ~p ",
                  [Info]),
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknwon}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
