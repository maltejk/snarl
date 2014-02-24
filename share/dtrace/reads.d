#!/usr/sbin/dtrace -s
/*
 * arg0 arg1 arg2 arg3 arg4  arg5 arg6   arg7 arg8
 * PID       ID   Type Found      Module Key  Opperation
 *
 * PID -> Erlang process ID. (String)
 * ID -> ID of the mesurement group, 801 for cowboy handler calls.
 * Found -> 1 if the item was found 2 if not
 * Type -> Indicates of this is a entry (1) or return (2).
 * Module - The module that was called. (string).
 *
 * run with: dtrace -s reads.d
 */

/*
 * This function gets called every time a erlang developper probe is
 * fiered, we filter for 801 and 1, so it gets executed when a handler
 * function is entered.
 */

erlang*:::user_trace*
/ arg2 == 4202 && arg3 == 2 && arg4 == 1/
{
  /*
   * We cache the relevant strings
   */
  @["found"] = count();
}


erlang*:::user_trace*
/ arg2 == 4202 && arg3 == 2 && arg4 == 2/
{
  /*
   * We cache the relevant strings
   */
  @["not found"] = count();
}
tick-1s
{
  printa(@);
}
