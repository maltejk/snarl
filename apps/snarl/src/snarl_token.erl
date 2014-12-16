-module(snarl_token).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         get/2,
         add/2,
         delete/2,
         reindex/2
        ]).

-ignore_xref([
              reindex/2
             ]).


-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {snarl, token, Met},
          Mod, Fun, Args)).

%% Public API
-spec get(binary(), fifo:token()) ->
                 not_found |
                 {ok, fifo:user_id()}.

reindex(_, _) -> ok.

get(Realm, Token) ->
    R = ?FM(get, snarl_entity_read_fsm, start,
            [{snarl_token_vnode, snarl_token}, get, {Realm, Token}]),
    case R of
        {ok, not_found} ->
            lager:debug("[token:~s] ~s not found.", [Realm, Token]),
            not_found;
        _ ->
            lager:debug("[token:~s] ~s found.", [Realm, Token]),
            R
    end.

add(Realm, User) ->
    case do_write(Realm, uuid:uuid4s(), add, User) of
        {ok, Token} ->
            lager:debug("[token:~s/~s] New token ~s.", [Realm, User, Token]),
            {ok, Token};
        E ->
            lager:debug("[token:~s/~s] Erroor ~p.", [Realm, User, E]),
            E
    end.

delete(Realm, Token) ->
    lager:debug("[token:~s] deleted token ~s.", [Realm, Token]),
    do_write(Realm, Token, delete).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Realm, Token, Op) ->
    ?FM(Op, snarl_entity_write_fsm, write,
        [{snarl_token_vnode, snarl_token}, {Realm, Token}, Op]).

do_write(Realm, Token, Op, Val) ->
    ?FM(Op, snarl_entity_write_fsm, write,
        [{snarl_token_vnode, snarl_token}, {Realm, Token}, Op, Val]).
