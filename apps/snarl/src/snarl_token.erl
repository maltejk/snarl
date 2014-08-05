-module(snarl_token).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         get/2,
         add/2,
         delete/2
        ]).

%% Public API

get(Realm, Token) ->
    snarl_entity_read_fsm:start(
      {snarl_token_vnode, snarl_token},
      get, {Realm, Token}).

add(Realm, User) ->
    do_write(Realm, uuid:uuid4s(), add, User).

delete(Realm, Token) ->
    do_write(Realm, Token, delete).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Realm, Token, Op) ->
    snarl_entity_write_fsm:write({snarl_token_vnode, snarl_token},
                                 {Realm, Token}, Op).

do_write(Realm, Token, Op, Val) ->
    snarl_entity_write_fsm:write({snarl_token_vnode, snarl_token},
                                 {Realm, Token}, Op, Val).
