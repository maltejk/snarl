-module(snarl_yubico).

-export([id/1, verify/1]).
-ignore_xref([id/1, verify/1]).

id(OTP) ->
    list_to_binary(yubico:yubikey_id(OTP)).

verify(OTP) ->
    ClientID = snarl_opt:get(yubico, api, client_id, yubico_client_id,
                             undefined),
    SecretKey = snarl_opt:get(yubico, api, secret_key, yubico_secret_key,
                             undefined),
    yubico:simple_verify(OTP, ClientID, SecretKey, []).
