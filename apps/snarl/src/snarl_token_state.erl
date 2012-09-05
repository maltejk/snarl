%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(snarl_token_state).

-include("snarl.hrl").

-export([
	 new/0,
	 user/2
	]).

new() ->
    <<"">>.

user(User, _Token) ->
    User.
