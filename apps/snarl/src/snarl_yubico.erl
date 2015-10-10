-module(snarl_yubico).

-export([id/1, verify/1]).
-ignore_xref([id/1, verify/1]).

id(OTP) ->
    list_to_binary(yubico:yubikey_id(OTP)).

verify(OTP) ->
    ClientIDi = snarl_opt:get(yubico, api, client_id),
    ClientID = integer_to_list(ClientIDi),
    SecretKey = snarl_opt:get(yubico, api, secret_key),
    yubico:simple_verify(OTP, ClientID, SecretKey, []).
