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
    {ok, ClientID} = application:get_env(snarl, yubico_client_id),
    {ok, SecretKey} = application:get_env(snarl, yubico_secret_key),
    yubico:simple_verify(OTP, ClientID, SecretKey, []).
