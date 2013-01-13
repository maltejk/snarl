%% @doc Interface for snarl-admin commands.
-module(snarl_console).
-export([join/1,
         leave/1,
         remove/1,
         ringready/1]).

-export([add_user/1,
	 add_group/1,
	 join_group/1,
	 leave_group/1,
	 grant_group/1,
	 grant_user/1,
	 revoke_user/1,
	 revoke_group/1,
	 passwd/1]).

-ignore_xref([
              join/1,
              leave/1,
              remove/1,
              ringready/1,
	      add_user/1,
	      add_group/1,
	      join_group/1,
	      leave_group/1,
	      grant_group/1,
	      grant_user/1,
	      revoke_user/1,
	      revoke_group/1,
	      passwd/1
             ]).

add_user([User]) ->
    case snarl_user:add(list_to_binary(User)) of
	ok ->
	    io:format("User '~s' added.~n", [User]),
	    ok;
	duplicate ->
	    io:format("User '~s' already exists.~n", [User]),
	    error
    end.

add_group([Group]) ->
    case snarl_group:add(list_to_binary(Group)) of
	ok ->
	    io:format("Group '~s' added.~n", [Group]),
	    ok;
	duplicate ->
	    io:format("Group '~s' already exists.~n", [Group]),
	    error
    end.

join_group([User, Group]) ->
    case snarl_user:lookup(list_to_binary(User)) of
        {ok, UserUUID} ->
            case snarl_user:join(UserUUID, list_to_binary(Group)) of
                {ok, joined} ->
                    io:format("User '~s' added to group '~s'.~n", [User, Group]),
                    ok;
                not_found ->
                    io:format("Group does not exist.~n"),
                    error
            end;
        _ ->
            io:format("User does not exist.~n"),
            error
    end.

leave_group([User, Group]) ->
    case snarl_user:lookup(list_to_binary(User)) of
        {ok, UserUUID} ->
            case snarl_user:leave(UserUUID, list_to_binary(Group)) of
                ok ->
                    io:format("User '~s' removed from group '~s'.~n", [User, Group]),
                    ok;
                not_found ->
                    io:format("Group does not exist.~n"),
                    error
            end;
        _ ->
            io:format("User does not exist.~n"),
            error
    end.

passwd([User, Pass]) ->
    case snarl_user:lookup(list_to_binary(User)) of
        {ok, UserUUID} ->
            case snarl_user:passwd(UserUUID, list_to_binary(Pass)) of
                ok ->
                    io:format("Password successfully changed for user '~s'.~n", [User]),
                    ok;
                not_found ->
                    io:format("User '~s' not found.~n", [User]),
                    error
            end;
        _ ->
            io:format("User does not exist.~n"),
            error
    end.

grant_group([Group | P]) ->
    case snarl_group:grant(list_to_binary(Group), build_permission(P)) of
	ok ->
	    io:format("Granted.~n", []),
	    ok;
	not_found ->
	    io:format("Group '~s' not found.~n", [Group]),
	    error
    end.

grant_user([User | P ]) ->
    case snarl_user:lookup(list_to_binary(User)) of
        {ok, UserUUID} ->
            case snarl_user:grant(UserUUID, build_permission(P)) of
                ok ->
                    io:format("Granted.~n", []),
                    ok;
                not_found ->
                    io:format("User '~s' not found.~n", [User]),
                    error
            end;
        _ ->
            io:format("User does not exist.~n"),
            error
    end.

revoke_user([User | P ]) ->
    case snarl_user:lookup(list_to_binary(User)) of
        {ok, UserUUID} ->
            case snarl_user:revoke(UserUUID, build_permission(P)) of
                ok ->
                    io:format("Granted.~n", []),
                    ok;
                not_found ->
                    io:format("User '~s' not found.~n", [User]),
                    error
            end;
        _ ->
            io:format("User does not exist.~n"),
            error
    end.

revoke_group([Group | P]) ->
    case snarl_group:revoke(list_to_binary(Group), build_permission(P)) of
	ok ->
	    io:format("Granted.~n", []),
	    ok;
	not_found ->
	    io:format("Group '~s' not found.~n", [Group]),
	    error
    end.

join([NodeStr]) ->
    try riak_core:join(NodeStr) of
        ok ->
            io:format("Sent join request to ~s\n", [NodeStr]),
            ok;
        {error, not_reachable} ->
            io:format("Node ~s is not reachable!\n", [NodeStr]),
            error;
        {error, different_ring_sizes} ->
            io:format("Failed: ~s has a different ring_creation_size~n",
                      [NodeStr]),
            error
    catch
        Exception:Reason ->
            lager:error("Join failed ~p:~p", [Exception, Reason]),
            io:format("Join failed, see log for details~n"),
            error
    end.

leave([]) ->
    remove_node(node()).

remove([Node]) ->
    remove_node(list_to_atom(Node)).

remove_node(Node) when is_atom(Node) ->
    try catch(riak_core:remove_from_cluster(Node)) of
        {'EXIT', {badarg, [{erlang, hd, [[]]}|_]}} ->
            %% This is a workaround because
            %% riak_core_gossip:remove_from_cluster doesn't check if
            %% the result of subtracting the current node from the
            %% cluster member list results in the empty list. When
            %% that code gets refactored this can probably go away.
            io:format("Leave failed, this node is the only member.~n"),
            error;
        Res ->
            io:format(" ~p\n", [Res])
    catch
        Exception:Reason ->
            lager:error("Leave failed ~p:~p", [Exception, Reason]),
            io:format("Leave failed, see log for details~n"),
            error
    end.

-spec(ringready([]) -> ok | error).
ringready([]) ->
    try riak_core_status:ringready() of
        {ok, Nodes} ->
            io:format("TRUE All nodes agree on the ring ~p\n", [Nodes]);
        {error, {different_owners, N1, N2}} ->
            io:format("FALSE Node ~p and ~p list different partition owners\n",
                      [N1, N2]),
            error;
        {error, {nodes_down, Down}} ->
            io:format("FALSE ~p down.  All nodes need to be up to check.\n",
                      [Down]),
            error
    catch
        Exception:Reason ->
            lager:error("Ringready failed ~p:~p", [Exception, Reason]),
            io:format("Ringready failed, see log for details~n"),
            error
    end.


build_permission(P) ->
    lists:map(fun ("...") ->
		      '...';
		  ("_") ->
		      '_';
		  (E) ->
		      list_to_binary(E)
	      end, P).

