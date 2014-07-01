-module(snarl_opt).

-ifdef(TEST).
-export([valid_type/2]).
-endif.

-export([get/5, set/2]).

get(Prefix, SubPrefix, Key, EnvKey, Dflt) ->
    case riak_core_metadata:get({Prefix, SubPrefix}, Key) of
        undefined ->
            V = case application:get_env(snarl, EnvKey) of
                    {ok, Val} ->
                        Val;
                    undefined ->
                        Dflt
                end,
            set(Prefix, SubPrefix, Key, V),
            V;
        V ->
            V
    end.

set(Ks, Val) ->
    case is_valid(Ks, Val) of
        {true, V1} ->
            [Prefix, SubPrefix, Key] =
                [list_to_atom(K) || K <- Ks],
            set(Prefix, SubPrefix, Key, V1);
        E ->
            E
    end.

set(Prefix, SubPrefix, Key, Val) ->
    riak_core_metadata:put({Prefix, SubPrefix}, Key, Val).

is_valid(Ks, V) ->
    Ks1 = [snarl_util:ensure_str(K) || K <- Ks],
    case get_type(Ks1) of
        {ok, Type} ->
            case  valid_type(Type, V) of
                {true, V1} ->
                    {true, V1};
                false ->
                    {invalid, type, Type}
            end;
        E ->
            E
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

get_type(Ks) ->
    get_type(Ks, opts()).

get_type([], _) ->
    {invalid, path};

get_type([K], Os) ->
    case proplists:get_value(K, Os) of
        undefined ->
            {invalid, key, K};
        Type ->
            {ok, Type}
    end;

get_type([K|R], Os) ->
    case proplists:get_value(K, Os) of
        undefined ->
            {invalid, key, K};
        Os1 ->
            get_type(R, Os1)
    end.

opts() ->
    [{"defaults",
      [{"users", [{"inital_role", binary}, {"inital_org", binary}]}]},
     {"yubico",
      [{"api", [{"client_id", integer}, {"secret_key", binary}]}]}].

valid_type(integer, I) when is_list(I) ->
    try list_to_integer(I) of
        V ->
            {true, V}
    catch
        _:_ ->
            false
    end;
valid_type(integer, I) when is_integer(I) ->
    {true, I};

valid_type(string, L) when is_binary(L) ->
    {true, binary_to_list(L)};
valid_type(string, L) when is_list(L) ->
    {true, L};

valid_type(binary, B) when is_binary(B) ->
    {true, B};
valid_type(binary, L) when is_list(L) ->
    {true, list_to_binary(L)};

valid_type({enum, Vs}, V) when is_atom(V)->
    valid_type({enum, Vs}, atom_to_list(V));
valid_type({enum, Vs}, V) when is_binary(V)->
    valid_type({enum, Vs}, binary_to_list(V));
valid_type({enum, Vs}, V) ->
    case lists:member(V, Vs) of
        true ->
            {true, list_to_atom(V)};
        false ->
            false
    end;

valid_type(_, _) ->
    false.
