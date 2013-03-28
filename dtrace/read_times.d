/*
 * arg0 arg1 arg2 arg3 arg4  arg5 arg6   arg7 arg8
 * PID       ID   Type Found      Module Key  Opperation
 *
 * PID -> Erlang process ID. (String)
 * ID -> ID of the mesurement group, 801 for cowboy handler calls.
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

erlang*:::user_trace-i4s4
/ arg2 == 4202 && arg3 == 1 /
{
  /*
   * We cache the relevant strings
   */
   self->_t[copyinstr(arg0), copyinstr(arg8), copyinstr(arg7)] = timestamp;
}

erlang*:::user_trace-i4s4
/ arg2 == 4202 && arg3 == 2 /
{
  /*
   * We cache the relevant strings
   */
  op = copyinstr(arg8);
  key = copyinstr(arg7);
  @[op] = quantize((timestamp - self->_t[copyinstr(arg0), op, key])/100000);
}


tick-1s
{
printa(@);
}