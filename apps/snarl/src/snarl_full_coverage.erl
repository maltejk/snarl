-module(snarl_full_coverage).

-include("snarl.hrl").

-behaviour(riak_core_coverage_fsm).

-export([
         init/2,
         process_results/2,
         finish/2,
         start/3
        ]).

-ignore_xref([start/3]).

-record(state, {replies, r, reqid, from, reqs, raw = false}).

start(VNodeMaster, NodeCheckService, {list, Realm, Requirements, Full}) ->
    start(VNodeMaster, NodeCheckService,
          {list, Realm, Requirements, Full, false});

start(VNodeMaster, NodeCheckService, {list, Realm, Requirements, false, _}) ->
    snarl_coverage:start(VNodeMaster, NodeCheckService,
                         {list, Realm, Requirements});

start(VNodeMaster, NodeCheckService,
      Request = {list, _Realm, Requirements, true, _}) ->
    ReqID = snarl_vnode:mk_reqid(),
    snarl_entity_coverage_fsm_sup:start_coverage(
      ?MODULE, {self(), ReqID, Requirements},
      {VNodeMaster, NodeCheckService, Request}),
    receive
        ok ->
            ok;
        {ok, Result} ->
            {ok, Result}
    after 10000 ->
            {error, timeout}
    end.

%% The first is the vnode service used
init({From, ReqID, Requirements}, {VNodeMaster, NodeCheckService,
                                   {Cmd, Realm, Requirements, Full, Raw}}) ->
    NVal = ?N,
    R = ?R,
    %% all - full coverage; allup - partial coverage
    VNodeSelector = allup,
    %% Same as R value here, TODO: Make this dynamic
    PrimaryVNodeCoverage = R,
    %% We timeout after 5s
    Timeout = 5000,
    State = #state{replies = dict:new(), r = R,
                   from = From, reqid = ReqID,
                   reqs = Requirements, raw = Raw},
    Request = {Cmd, Realm, Requirements, Full},
    {Request, VNodeSelector, NVal, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, State}.

process_results({ok, _ReqID, _IdxNode, Obj},
                State = #state{replies = Replies}) ->
    lager:debug("Objs: ~p", [Obj]),
    Replies1 = lists:foldl(fun ({Pts, {Key, V}}, D) ->
                                   dict:append(Key, {Pts, V}, D)
                           end, Replies, Obj),
    {done, State#state{replies = Replies1}};

process_results(Result, State) ->
    lager:error("Unknown process results call: ~p ~p", [Result, State]),
    {done, State}.

finish(clean, State = #state{replies = Replies,
                             from = From, r = R}) ->
    MergedReplies = dict:fold(fun(_Key, Es, Res) when length(Es) < R->
                                      Res;
                                 (_Key, Es, Res) ->
                                      Mgd = case State#state.raw of
                                                false ->
                                                    merge(Es);
                                                true ->
                                                    raw_merge(Es)
                                            end,
                                      lager:debug("Merged: ~p-> ~p", [Es, Mgd]),
                                      [Mgd | Res]
                              end, [], Replies),
    From ! {ok, MergedReplies},
    {stop, normal, State};

finish(How, State) ->
    lager:error("Unknown process results call: ~p ~p", [How, State]),
    {error, failed}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

raw_merge([{Score, V} | R]) ->
    raw_merge(R, Score, [V]).

raw_merge([], recalculate, Vs) ->
    {0, ft_obj:merge(snarl_entity_read_fsm, Vs)};

raw_merge([], Score, Vs) ->
    {Score, ft_obj:merge(snarl_entity_read_fsm, Vs)};


raw_merge([{Score, V} | R], Score, Vs) ->
    raw_merge(R, Score, [V | Vs]);

raw_merge([{_Score1, V} | R], _Score2, Vs) when _Score1 =/= _Score2->
    raw_merge(R, recalculate, [V | Vs]).

merge([{Score, V} | R]) ->
    merge(R, Score, [V]).

merge([], recalculate, Vs) ->
    {0, merge_obj(Vs)};

merge([], Score, Vs) ->
    {Score, merge_obj(Vs)};


merge([{Score, V} | R], Score, Vs) ->
    merge(R, Score, [V | Vs]);

merge([{_Score1, V} | R], _Score2, Vs) when _Score1 =/= _Score2->
    merge(R, recalculate, [V | Vs]).


merge_obj(Vs) ->
    O = ft_obj:merge(snarl_entity_read_fsm, Vs),
    ft_obj:val(O).
