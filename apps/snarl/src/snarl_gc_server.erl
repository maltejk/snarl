%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2013, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 24 Jun 2013 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(snarl_gc_server).

-behaviour(gen_server).

-include("snarl.hrl").

%% API
-export([start_link/0,
         next/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ignore_xref([start_link/0,
              next/0]).

-define(SERVER, ?MODULE).

-record(state, {
          groups = [],
          orgs = [],
          interval = 0,
          total = 0,
          compacted = 0,
          cnt = 0,
          group_timeout,
          org_timeout,
          start = {0,0,0}
         }).

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
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

next() ->
    gen_server:cast(?SERVER, next).

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
init([]) ->
    case ?ENV(auto_gc, disabled) of
        disabled ->
            lager:warning("[Auto GC] disabled."),
            {ok, #state{}};
        I ->
            T = 1000 * I,
            lager:warning("[Auto GC] enabled one object every ~ps.", [I]),
            timer:apply_interval(T, ?MODULE, next, []),
            {GroupT, GroupU} = env(group_sync_timeout, {1, week}),
            {OrgT, OrgU} = env(org_sync_timeout, {1, week}),
            {ok, #state{
                    start = os:timestamp(),
                    group_timeout = time_to_us(GroupT,
                                               atom_to_list(GroupU)),
                    org_timeout = time_to_us(OrgT,
                                              atom_to_list(OrgU))
                   }}
    end.

env(K, D) ->
    ?ENV(K, D).

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

handle_cast(next, State = #state{
                             start = Start,
                             compacted = C,
                             cnt = Cnt,
                             groups = [],
                             orgs = []}) ->
    T0 = os:timestamp(),
    Td = timer:now_diff(T0, Start) / 1000000,
    lager:info("[Auto GC] Run complete in ~ps.", [Td]),
    lager:info("[Auto GC] Saved a total of ~p byte in ~p objects.", [C, Cnt]),
    {ok, Groups} = snarl_group:list(),
    {ok, Orgs} = snarl_group:list(),
    lager:info("[Auto GC] Startign new run wiht ~p Groups and ~p Orgs.",
               [length(Groups), length(Orgs)]),
    {noreply,
     State#state{
       groups = Groups,
       orgs = Orgs,
       start = T0,
       cnt = 0,
       compacted = 0
      }};

handle_cast(next, State = #state{
                             compacted = C,
                             cnt = Cnt,
                             group_timeout = Timeout,
                             groups = [UUID | Gs]}) ->
    case snarl_group:gcable(UUID) of
        {ok, A} ->
            MinAge = ecrdt:timestamp_us() - Timeout,
            A1 = [E || {{T,_},_} = E <- A, T < MinAge],
            {ok, Size} = snarl_group:gc(UUID, A1),
            {noreply,
             State#state{
               groups = Gs,
               cnt = Cnt + 1,
               compacted = C + Size
              }};
        _ ->
            {noreply,
             State#state{
               groups = Gs
              }}
    end;

handle_cast(next, State = #state{
                             compacted = C,
                             cnt = Cnt,
                             groups = [],
                             org_timeout = Timeout,
                             orgs = [UUID | Os]}) ->
    case snarl_org:gcable(UUID) of
        {ok, A} ->
            MinAge = ecrdt:timestamp_us() - Timeout,
            A1 = [E || {{T,_},_} = E <- A, T < MinAge],
            {ok, Size} = snarl_org:gc(UUID, A1),
            {noreply,
             State#state{
               orgs = Os,
               cnt = Cnt + 1,
               compacted = C + Size
              }};
        _ ->
            {noreply,
             State#state{
               groups = Os
              }}
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

time_to_us(Time, [$s | _]) ->
    Time * ?SECOND;
time_to_us(Time, [$m | _]) ->
    Time * ?MINUTE;
time_to_us(Time, [$h | _]) ->
    Time * ?HOUER;
time_to_us(Time, [$d | _]) ->
    Time * ?DAY;
time_to_us(Time, [$w | _]) ->
    Time * ?WEEK.
