%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  5 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(snarl_sync).

-behaviour(gen_server).

%% API
-export([start/2, start_link/2, sync_op/6]).

-ignore_xref([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {ip, port, socket, timeout}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------

start(IP, Port) ->
    snarl_sync_sup:start_child(IP, Port).

start_link(IP, Port) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [IP, Port], []).

sync_op(Node, VNode, System, User, Op, Val) ->
    gen_server:abcast(?SERVER, {write, Node, VNode, System, User, Op, Val}).

reconnect() ->
    reconnect(self()).

reconnect(Pid) ->
    gen_server:cast(Pid, reconnect).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([IP, Port]) ->
    Timeout = case application:get_env(sync_recv_timeout) of
                  {ok, T} ->
                      T;
                  _ ->
                      1500
              end,
    case gen_tcp:connect(IP, Port,
                         [binary, {active,false}, {packet,4}],
                         Timeout) of
        {ok, Socket} ->
            {ok, #state{socket=Socket, ip=IP, port=Port, timeout=Timeout}};
        E ->
            lager:error("[sync] Initialization failed: ~p.", [E]),
            reconnect(self()),
            {ok, #state{ip=IP, port=Port, timeout=Timeout}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({write, Node, VNode, System, User, Op, Val}, State = #state{socket=undefined}) ->
    lager:debug("[sync] ~p", [{write, Node, VNode, System, User, Op, Val}]),
    {noreply, State};
handle_cast({write, Node, VNode, System, User, Op, Val}, State = #state{socket=Socket}) ->
    Command = {write, Node, VNode, System, User, Op, Val},
    case gen_tcp:send(Socket, term_to_binary(Command)) of
        ok ->
            ok;
        E ->
            lager:error("[sync] Error: ~p", [E]),
            reconnect()
    end,
    {noreply, State};

handle_cast(reconnect, State = #state{ip=IP, port=Port, timeout=Timeout}) ->
    timer:sleep(500),
    case gen_tcp:connect(IP, Port,
                         [binary, {active,false}, {packet,4}],
                         Timeout) of
        {ok, Socket} ->
            {ok, State = #state{socket=Socket}};
        E ->
            lager:error("[sync] Initialization failed: ~p.", [E]),
            reconnect(self()),
            {ok, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
