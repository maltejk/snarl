-module(snarl_yubico).

-export([id/1, verify/1]).
-ignore_xref([id/1, verify/1]).

id(OTP) ->
    try
        list_to_binary(yubico:yubikey_id(ensure_str(OTP)))
    catch
        _:_ ->
            <<>>
    end.

verify(OTP) ->
    ClientID = snarl_opt:get(yubico, api, client_id, yubico_client_id,
                             undefined),
    SecretKey = snarl_opt:get(yubico, api, secret_key, yubico_secret_key,
                             undefined),
    yubico:simple_verify(ensure_str(OTP), ensure_str(ClientID),
						 ensure_str(SecretKey), []).

ensure_str(B) when is_binary(B) ->
	binary_to_list(B);
ensure_str(L) when is_list(L) ->
	L.
