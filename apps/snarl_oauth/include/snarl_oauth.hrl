-define(ACCESS_CODE_TABLE, access_codes).
-define(ACCESS_TOKEN_TABLE, access_tokens).
-define(REFRESH_TOKEN_TABLE, refresh_tokens).
-define(REQUEST_TABLE, requests).

-record(oauth_state,
        {
          realm :: binary()
        }).
