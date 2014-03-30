-module(snarl_yubico).

-export([id/1, verify/1]).
-ignore_xref([id/1, verify/1]).

id(OTP) when is_binary(OTP) ->
    id(binary_to_list(OTP));
id(OTP) ->
    try
        list_to_binary(yubico:yubikey_id(OTP))
    catch
        _:_ ->
            <<>>
    end.

verify(OTP) when is_binary(OTP) ->
    verify(binary_to_list(OTP));

verify(OTP) ->
    ClientID = snarl_opt:get(yubico, api, client_id, yubico_client_id,
                             undefined),
    SecretKey = snarl_opt:get(yubico, api, secret_key, yubico_secret_key,
                             undefined),
    yubico:simple_verify(OTP, ClientID, SecretKey, []).
