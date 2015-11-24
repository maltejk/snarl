-module(snarl_indexed).

-export([reindex/0]).

-callback reindex(Realm :: binary(), UUID :: binary()) ->
    ok.

reindex() ->
    lager:info("[reindex] Starting reindex, giving the cluste 5s for the "
               "cluster to start up."),
    timer:sleep(5000),
    Start = erlang:monotonic_time(micro_seconds),
    lager:info("[reindex] Gathering elements to reindex..."),
    {ok, Users} = snarl_user:list(),
    Users1 = [{snarl_user, U} || U <- Users],
    {ok, Clients} = snarl_client:list(),
    Clients1 = [{snarl_client, U} || U <- Clients],
    {ok, Orgs} = snarl_org:list(),
    Orgs1 = [{snarl_org, O} || O <- Orgs],
    {ok, Roles} = snarl_role:list(),
    Roles1 = [{snarl_role, R} || R <- Roles],
    All = lists:flatten([Clients1, Orgs1, Roles1], Users1),
    Cnt = length(All),
    lager:info("[reindex] A total of ~p elements are going to be reindexed...",
               [Cnt]),
    lists:foldl(fun do_index/2, {0, 0, Cnt}, All),
    End = erlang:monotonic_time(micro_seconds),
    lager:info("[reindex] Reindex completed after ~.2fms...",
               [(End - Start)/1000]).

do_index({Mod, {Realm, UUID}}, {P, Cnt, Max}) ->
    timer:sleep(100),
    Mod:reindex(Realm, UUID),
    Cnt1 = Cnt + 1,
    case trunc((Cnt1 / Max) * 10) of
        P1 when P1 > P ->
            lager:info("[reindex] ~p% (~p)", [P1*10, Cnt1]),
            {P1, Cnt1, Max};
        _ ->
            {P, Cnt1, Max}
    end.
