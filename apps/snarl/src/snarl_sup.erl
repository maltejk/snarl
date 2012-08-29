-module(snarl_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    VMaster = {snarl_vnode_master,
	       {riak_core_vnode_master, start_link, [snarl_vnode]},
	       permanent, 5000, worker, [riak_core_vnode_master]},
    
    GroupVMaster = {snarl_group_vnode_master,
		    {riak_core_vnode_master, start_link, [snarl_group_vnode]},
		    permanent, 5000, worker, [riak_core_vnode_master]},
    
    UserVMaster = {snarl_user_vnode_master,
		   {riak_core_vnode_master, start_link, [snarl_user_vnode]},
		   permanent, 5000, worker, [riak_core_vnode_master]},
    
    WriteFSMs = {snarl_entity_write_fsm_sup,
		{snarl_entity_write_fsm_sup, start_link, []},
		permanent, infinity, supervisor, [snarl_entity_write_fsm_sup]},

    CoverageFSMs = {snarl_entity_coverage_fsm_sup,
		    {snarl_entity_coverage_fsm_sup, start_link, []},
		    permanent, infinity, supervisor, [snarl_entity_coverage_fsm_sup]},
    
    ReadFSMs = {snarl_entity_read_fsm_sup,
	       {snarl_entity_read_fsm_sup, start_link, []},
	       permanent, infinity, supervisor, [snarl_entity_read_fsm_sup]},

    {ok,
     {{one_for_one, 5, 10},
      [VMaster, 
       GroupVMaster, UserVMaster,
       ReadFSMs, WriteFSMs, CoverageFSMs]}}.
