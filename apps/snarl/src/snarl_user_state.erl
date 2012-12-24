%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2012, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Aug 2012 by Heinz Nikolaus Gies <heinz@licenser.net>

-module(snarl_user_state).

-include("snarl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-export([
	 new/0,
	 name/2,
	 passwd/2,
	 grant/2,
	 revoke/2,
	 join/2,
	 leave/2,
	 add/2,
	 delete/2,
	 set_resource/3,
	 get_resource/2,
	 claim_resource/4,
	 free_resource/3,
	 get_free_resource/2,
	 get_resource_stat/1
	]).

-ignore_xref([claim_resource/4,
	      free_resource/3,
	      get_resource/2,
	      grant/2,
	      join/2,
	      leave/2,
	      passwd/2,
	      revoke/2,
	      set_resource/3]).
new() ->
    #user{}.

name(Name, User) ->
    User#user{name = Name}.

passwd(Passwd, User) ->
    User#user{passwd = crypto:sha([User#user.name, Passwd])}.

grant(Permission, User) ->
    User#user{permissions = ordsets:add_element(Permission, User#user.permissions)}.

revoke(Permission, User) ->
    User#user{permissions = ordsets:del_element(Permission, User#user.permissions)}.

join(Group, User) ->
    User#user{groups = ordsets:add_element(Group, User#user.groups)}.

leave(Group, User) ->
    User#user{groups = ordsets:del_element(Group, User#user.groups)}.

set_resource(Resource, Granted, User) ->
    R = #resource{
      name = Resource,
      granted = Granted
     },
    Resources0 = lists:keydelete(Resource, 2, User#user.resources),
    User#user{resources = ordsets:add_element(R, Resources0)}.

claim_resource(Resource, ID, Ammount, User) ->
    Claim = #resource_claim{
      id = ID,
      ammount = Ammount
     },
    Resources0 = User#user.resources,
    {_, R0, Resources1} = lists:keytake(Resource, 2, Resources0),
    R1 = R0#resource{claims =  ordsets:add_element(Claim, R0#resource.claims)},
    User#user{resources = ordsets:add_element(R1, Resources1)}.

free_resource(Resource, ID, User) ->
    Resources0 = User#user.resources,
    {_, R0, Resources1} = lists:keytake(Resource, 2, Resources0),
    R1 = R0#resource{claims =  lists:keydelete(ID, 2, R0#resource.claims)},
    User#user{resources =  ordsets:add_element(R1, Resources1)}. 

add(User, Users) ->
    ordsets:add_element(User, Users).

delete(User, Users) ->
    ordsets:del_element(User, Users).

get_resource_stat(User) ->

    lists:map(fun(R) ->
		      Used = lists:foldl(fun(#resource_claim{ammount=In}, Acc) ->
						 Acc + In
					 end, 0, R#resource.claims),
		      Reserved = lists:foldl(fun({#resource_claim{ammount=In}, _}, Acc) ->
						     Acc + In
					     end, 0, R#resource.reservations),
		      {R#resource.name,
		       R#resource.granted,
		       Used,
		       Reserved}
	      end, User#user.resources).

get_resource(Resource, User) ->
    lists:keyfind(Resource, 2, User#user.resources).

get_free_resource(Resource, User) ->
    R = lists:keyfind(Resource, 2, User#user.resources),
    Used = lists:foldl(fun(#resource_claim{ammount=In}, Acc) ->
			       Acc + In
		       end, 0, R#resource.claims),
    Reserved = lists:foldl(fun({#resource_claim{ammount=In}, _}, Acc) ->
				   Acc + In
			   end, 0, R#resource.reservations),

    R#resource.granted - Used - Reserved.


-ifdef(TEST).
name_test() ->
    Name0 = "Test0",
    User0 = new(),
    User1 = name(Name0, User0),
    Name1 = "Test1",
    User2 = name(Name1, User1),
    ?assert(Name0 == User1#user.name),
    ?assert(Name1 == User2#user.name).

passwd_test() ->
    Name = "Test",
    Passwd = "Test",
    Hash = crypto:sha([Name, Passwd]),
    User0 = new(),
    User1 = name(Name, User0),
    User2 = passwd(Passwd, User1),
    ?assert(Hash == User2#user.passwd).

permissions_test() ->
    P0 = ["P0"],
    P1 = ["P1"],
    User0 = new(),
    User1 = grant(P0, User0),
    User2 = grant(P1, User1),
    User3 = grant(P0, User2),
    User4 = revoke(P0, User3),
    User5 = revoke(P1, User3),
    ?assert([P0] == User1#user.permissions),
    ?assert([P0, P1] == User2#user.permissions),
    ?assert([P0, P1] == User3#user.permissions),
    ?assert([P1] == User4#user.permissions),
    ?assert([P0] == User5#user.permissions).

groups_test() ->
    G0 = "G0",
    G1 = "G1",
    User0 = new(),
    User1 = join(G0, User0),
    User2 = join(G1, User1),
    User3 = join(G0, User2),
    User4 = leave(G0, User3),
    User5 = leave(G1, User3),
    ?assert(User1#user.groups == [G0]),
    ?assert(User2#user.groups == [G0, G1]),
    ?assert(User3#user.groups == [G0, G1]),
    ?assert(User4#user.groups == [G1]),
    ?assert(User5#user.groups == [G0]).

resource_test() ->
    User0 = new(),
    User1 = set_resource(cookies, 10, User0),
    User2 = set_resource(cookies, 20, User1),
    User3 = set_resource(cakes, 42, User2),

    R0 = get_resource(cookies, User1),
    R1 = get_resource(cookies, User3),
    R2 = get_resource(cakes, User3),

    ?assert(R0#resource.granted == 10),
    ?assert(R1#resource.granted == 20),
    ?assert(R2#resource.granted == 42).

resource_stat_test() ->
    User0 = new(),
    User1 = set_resource(cookies, 10, User0),
    User2 = claim_resource(cookies, c1, 1, User1),
    User3 = claim_resource(cookies, c2, 2, User2),
    User4 = free_resource(cookies, c1, User3),
    User5 = free_resource(cookies, c2, User3),
    User6 = free_resource(cookies, c1, User4),
    User7 = set_resource(cake, 5, User6),
    User8 = claim_resource(cake, c1, 1, User7),

    Free0 = get_resource_stat(User1),
    Free1 = get_resource_stat(User2),
    Free2 = get_resource_stat(User3),
    Free3 = get_resource_stat(User4),
    Free4 = get_resource_stat(User5),
    Free5 = get_resource_stat(User6),
    Free6 = get_resource_stat(User8),

    ?assert(Free0 == [{cookies, 10, 0, 0}]),
    ?assert(Free1 == [{cookies, 10, 1, 0}]),
    ?assert(Free2 == [{cookies, 10, 3, 0}]),
    ?assert(Free3 == [{cookies, 10, 2, 0}]),
    ?assert(Free4 == [{cookies, 10, 1, 0}]),
    ?assert(Free5 == [{cookies, 10, 2, 0}]),
    ?assert(Free6 == [{cake, 5, 1, 0}, {cookies, 10, 2, 0}]).

resource_claim_test() ->
    User0 = new(),
    User1 = set_resource(cookies, 10, User0),
    User2 = claim_resource(cookies, c1, 1, User1),
    User3 = claim_resource(cookies, c2, 2, User2),
    User4 = free_resource(cookies, c1, User3),
    User5 = free_resource(cookies, c2, User3),
    User6 = free_resource(cookies, c1, User4),

    Free0 = get_free_resource(cookies, User1),
    Free1 = get_free_resource(cookies, User2),
    Free2 = get_free_resource(cookies, User3),
    Free3 = get_free_resource(cookies, User4),
    Free4 = get_free_resource(cookies, User5),
    Free5 = get_free_resource(cookies, User6),

    ?assert(Free0 == 10),
    ?assert(Free1 == 9),
    ?assert(Free2 == 7),
    ?assert(Free3 == 8),
    ?assert(Free4 == 9),
    ?assert(Free5 == 8).

-endif.
