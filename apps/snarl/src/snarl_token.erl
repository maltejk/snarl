-module(snarl_token).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         get/2,
         add/2,
         delete/2
        ]).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {snarl, token, Met},
          Mod, Fun, Args)).

%% Public API
-spec get(binary(), fifo:token()) ->
                 not_found |
                 {ok, fifo:user_id()}.

get(Realm, Token) ->
    R = ?FM(get, snarl_entity_read_fsm, start,
            [{snarl_token_vnode, snarl_token}, get, {Realm, Token}]),
    case R of
        {ok, not_found} ->
            not_found;
        _ ->
            R
    end.

add(Realm, User) ->
    do_write(Realm, uuid:uuid4s(), add, User).

delete(Realm, Token) ->
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
