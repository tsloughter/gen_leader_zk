-module(gen_leader).

-export([start/4,
         start/5,
         start_link/4,
         start_link/5,
         leader_call/2,
         leader_call/3,
         leader_cast/2,
         call/2,
         call/3,
         cast/2,
         reply/2]).

-export([alive/1,
         down/1,
         candidates/1,
         workers/1]).

-export([init_it/6,
         print_event/3]).

-export([system_continue/3,
         system_terminate/4,
         system_code_change/4,
         format_status/2]).

-include_lib("erlzk/include/erlzk.hrl").

-record(election,{leader = none,
                  leader_ref,
                  zk,
                  mode = global,
                  name,
                  znode,
                  leadernode = none,
                  candidate_nodes = [],
                  worker_nodes = [],
                  alive = [],
                  iteration,
                  down = [],
                  monitored = [],
                  buffered = []
                 }).

-record(server, {parent,
                 mod,
                 state,
                 debug}).

-type mod_state() :: any().
-type election() :: #election{}.
-type msg() :: any().
-type reply() :: any().
-type from() :: {pid(), _Tag :: any()}.
-type reason() :: any().
-type cb_return() ::
        {ok, mod_state()}
      | {ok, msg(), mod_state()}
      | {stop, reason, mod_state()}.
-type cb_reply() ::
        {reply, reply(), mod_state()}
      | {reply, reply(), msg(), mod_state()}
      | {noreply, mod_state()}
      | {stop, reason(), mod_state()}.

-callback init(any()) -> mod_state().
-callback elected(mod_state(), election()) -> cb_return() | {reply, msg(), mod_state()}.
-callback surrendered(mod_state(), msg(), election()) -> cb_return().
-callback handle_DOWN(pid(), mod_state(), election()) -> cb_return().
-callback handle_leader_call(msg(), from(), mod_state(), election()) -> cb_reply().
-callback handle_leader_cast(msg(), mod_state(), election()) -> cb_return().
-callback from_leader(msg(), mod_state(), election()) -> cb_return().
-callback handle_call(msg(), from(), mod_state(), election()) -> cb_reply().
-callback handle_cast(msg(), mod_state(), election()) -> cb_return().
-callback handle_info(msg(), mod_state(), election()) -> cb_return().

start(Name, Mod, Arg, Options) when is_atom(Name) ->
    gen:start(?MODULE, nolink, {local, Name}, Mod, {local_only, Arg}, Options).

start(Name, GroupName, Mod, Arg, Options)
  when is_atom(Name) ->
    gen:start(?MODULE, nolink, {local, Name}, Mod, {GroupName, Arg}, Options).

start_link(Name, GroupName, Mod, Arg, Options) when is_atom(Name) ->
    gen:start(?MODULE, link, {local, Name}, Mod, {GroupName, Arg}, Options).

start_link(Name, Mod, Arg, Options) when is_atom(Name) ->
    gen:start(?MODULE, link, {local, Name}, Mod, {local_only, Arg}, Options).

%% Query functions

alive(#election{alive=Alive}) ->
    Alive.

down(#election{down=Down}) ->
    Down.

candidates(#election{candidate_nodes=Cands}) ->
    Cands.

workers(#election{worker_nodes=Workers}) ->
    Workers.

%% call functions

call(Name, Request) ->
    case catch gen:call(Name, '$gen_call', Request) of
        {ok, Res} ->
            Res;
        {'EXIT', Reason} ->
            exit({Reason, {?MODULE, local_call, [Name, Request]}})
    end.

call(Name, Request, Timeout) ->
    case catch gen:call(Name, '$gen_call', Request, Timeout) of
        {ok, Res} ->
            Res;
        {'EXIT', Reason} ->
            exit({Reason, {?MODULE, local_call, [Name, Request, Timeout]}})
    end.

leader_call(Name, Request) ->
    case catch gen:call(Name, '$leader_call', Request) of
        {ok,{leader,reply,Res}} ->
            Res;
        {ok,{error, leader_died}} ->
            exit({leader_died, {?MODULE, leader_call, [Name, Request]}});
        {'EXIT',Reason} ->
            exit({Reason, {?MODULE, leader_call, [Name, Request]}})
    end.

leader_call(Name, Request, Timeout) ->
    case catch gen:call(Name, '$leader_call', Request, Timeout) of
        {ok, {leader, reply, Res}} ->
            Res;
        {ok, {error, leader_died}} ->
            exit({leader_died, {?MODULE, leader_call, [Name, Request]}});
        {'EXIT', Reason} ->
            exit({Reason, {?MODULE, leader_call, [Name, Request, Timeout]}})
    end.

%% Same as gen_server:cast
cast(Name, Request) ->
    catch do_cast('$gen_cast', Name, Request),
    ok.

leader_cast(Name, Request) ->
    catch do_cast('$leader_cast', Name, Request),
    ok.

do_cast(Tag, Name, Request) when is_atom(Name) ->
    Name ! {Tag, Request};
do_cast(Tag, Pid, Request) when is_pid(Pid) ->
    Pid ! {Tag, Request}.

reply({To, Tag}, Reply) ->
    catch To ! {Tag, Reply}.


%%% ---------------------------------------------------
%%% Initiate the new process.
%%% Register the name using the Rfunc function
%%% Calls the Mod:init/Args function.
%%% Finally an acknowledge is sent to Parent and the main
%%% loop is entered.
%%% ---------------------------------------------------
%%% @hidden
init_it(Starter, self, Name, Mod, {CandidateNodes, Arg}, Options) ->
    if CandidateNodes == [] ->
            erlang:error(no_candidates);
       true ->
            init_it(Starter, self(), Name, Mod,
                    {CandidateNodes, Arg}, Options)
    end;
init_it(Starter, Parent, Name, Mod, {local_only, _}=Arg, Options) ->
    Debug = debug_options(Name, Options),
    case catch Mod:init(Arg) of
        {stop, Reason} ->
            proc_lib:init_ack(Starter, {error, Reason}),
            exit(Reason);
        ignore ->
            proc_lib:init_ack(Starter, ignore),
            exit(normal);
        {'EXIT', Reason} ->
            proc_lib:init_ack(Starter, {error, Reason}),
            exit(Reason);
        {ok, State} ->
            proc_lib:init_ack(Starter, {ok, self()}),
            Server = #server{parent = Parent,
                             mod = Mod,
                             state = State,
                             debug = Debug},
            loop(Server, local_only, #election{name = Name, mode = local});
        Other ->
            Error = {bad_return_value, Other},
            proc_lib:init_ack(Starter, {error, Error}),
            exit(Error)
    end;
init_it(Starter, Parent, Name, Mod, {GroupName, Arg}, Options) ->
    ZKs = application:get_env(gen_leader_zk, zookeepers, [{"localhost", 2181}]),
    {ok, Pid} = erlzk:connect(ZKs, 30000, [{monitor, self()}]),
    Debug = debug_options(Name, Options),
    Election = #election{name = GroupName, zk=Pid},
    case catch Mod:init(Arg) of
        {stop, Reason} ->
            proc_lib:init_ack(Starter, {error, Reason}),
            exit(Reason);
        ignore ->
            proc_lib:init_ack(Starter, ignore),
            exit(normal);
        {'EXIT', Reason} ->
            proc_lib:init_ack(Starter, {error, Reason}),
            exit(Reason);
        {ok, State} ->
            proc_lib:init_ack(Starter, {ok, self()}),
            begin_election(#server{parent = Parent,
                                   mod = Mod,
                                   state = State,
                                   debug = Debug}, Election);
        Else ->
            Error = {bad_return_value, Else},
            proc_lib:init_ack(Starter, {error, Error}),
            exit(Error)
    end.

begin_election(Server, E=#election{name=GroupName,
                                   znode=undefined,
                                   zk=ZK}) ->
    GroupPath = filename:join("/", GroupName),
    ElectionPath = filename:join(GroupPath, "election"),
    ElectionZNode = filename:join(ElectionPath, "n_"),
    case erlzk:exists(ZK, ElectionPath) of
        {error, no_node} ->
            erlzk:create(ZK, GroupPath),
            erlzk:create(ZK, ElectionPath);
        _ ->
            ok
    end,

    {ok, ZNodePath} = erlzk:create(ZK, ElectionZNode, ephemeral_sequential),
    {ok, Stat} = erlzk:exists(ZK, ZNodePath),
    {ok, _} = erlzk:set_data(ZK, ZNodePath, term_to_binary(self()), Stat#stat.version),
    begin_election(Server, E#election{znode=ZNodePath});
begin_election(Server=#server{mod=Mod, state=State}, E=#election{name=GroupName,
                                                                 znode=ZNodePath,
                                                                 zk=ZK}) ->
    ZNode = filename:basename(ZNodePath),
    GroupPath = filename:join("/", GroupName),
    ElectionPath = filename:join(GroupPath, "election"),
    {ok, Children} = erlzk:get_children(ZK, ElectionPath),
    case lists:sort(Children) of
        [Leader | _] when ZNode =:= Leader ->
            %% I'm the leader
            {ok, _Synch, NewState} = Mod:elected(State, E),
            loop(Server#server{state = NewState}, leader, E#election{leader=self(), leader_ref=undefined});
        [LeaderNode | _] = _Children1 ->
            %% someone else is the leader

            %% We could/should just monitor the neighbot in ZK
            %% then we don't have to check if we are leader unless that
            %% node goes away. But then we'd need to message from the leader
            %% to all other nodes that it is the new leader.
            %% Neighbor = find_neighbor(Children1, ZNode),
            %% Path = filename:join(ElectionPath, Neighbor),

            LeaderPath = filename:join(ElectionPath, LeaderNode),
            {ok, {PidBinary, _Stat}} = erlzk:get_data(ZK, LeaderPath),
            Leader = binary_to_term(PidBinary),
            LeaderRef = erlang:monitor(process, Leader),
            erlzk:exists(ZK, LeaderPath, self()),
            loop(Server, follower, E#election{leader=Leader, leader_ref=LeaderRef})
    end.

%% find_neighbor([], _ZNode) ->
%%     erlang:error(no_candidates);
%% find_neighbor([H, H1 | _], ZNode) when H1 >= ZNode ->
%%     H;
%% find_neighbor([_ | T], ZNode) ->
%%     find_neighbor(T, ZNode).

%%% ---------------------------------------------------
%%% The MAIN loop.
%%% ---------------------------------------------------

loop(Server=#server{parent = Parent, debug = Debug}, Role, E=#election{mode=Mode,
                                                                       leader_ref=LeaderRef,
                                                                       leader=LeaderPid})->
    Msg = receive
              Input ->
                  Input
          end,
    case Msg of
        {'DOWN', LeaderRef, process, LeaderPid, _} ->
            %% current leader is down before we've been notified by ZK
            %% set it to undefined and wait for ZK to notice or
            %% in the case it continues to be connected to ZK despite being
            %% partitioned from this node, eventually crash
            loop(Server, Role, E#election{leader_ref=undefined, leader=undefined});
        {'DOWN', _, process, _, _} ->
            %% could get a DOWN from a former leader after electing a new one, simply ignore it.
            loop(Server, Role, E);
        {State, _Host, _Port} when State =:= disconnected ; State =:= expired ->
            %% Lost connection to ZK, meaning all ephemeral nodes are gone, which means we have no znode
            begin_election(Server, E#election{znode=undefined});
        {connected, _Host, _Port} ->
            %% Ignore
            loop(Server, Role, E);
        {_Op, _Path, node_deleted} ->
            begin_election(Server, E);
        {system, From, Req} ->
            sys:handle_system_msg(Req, From, Parent, ?MODULE, Debug,
                                  [normal, Server, Role, E]);
        {'EXIT', Parent, Reason} ->
            terminate(Reason, Msg, Server, Role, E);
        {leader, local_only, _, _Candidate} ->
            loop(Server, Role, E);
        LeaderMsg when element(1, LeaderMsg) == leader, Mode == local ->
            Candidate = element(size(LeaderMsg), LeaderMsg),
            Candidate ! {leader, local_only, node(), self()},
            loop(Server, Role, E);
        _Msg when Debug == [] ->
            handle_msg(Msg, Server, Role, E);
        _Msg ->
            Debug1 = sys:handle_debug(Debug, {?MODULE, print_event},
                                      E#election.name, {in, Msg}),
            handle_msg(Msg, Server#server{debug = Debug1}, Role, E)
    end.

%%-----------------------------------------------------------------
%% Callback functions for system messages handling.
%%-----------------------------------------------------------------

%% @hidden
system_continue(_Parent, Debug, [normal, Server, Role, E]) ->
    loop(Server#server{debug = Debug}, Role, E).

%% @hidden
system_terminate(Reason, _Parent, Debug, [_Mode, Server, Role, E]) ->
    terminate(Reason, [], Server#server{debug = Debug}, Role, E).

%% @hidden
system_code_change([Mode, Server, Role, E], _Module, OldVsn, Extra) ->
    #server{mod = Mod, state = State} = Server,
    case catch Mod:code_change(OldVsn, State, E, Extra) of
        {ok, NewState} ->
            NewServer = Server#server{state = NewState},
            {ok, [Mode, NewServer, Role, E]};
        {ok, NewState, NewE} ->
            NewServer = Server#server{state = NewState},
            {ok, [Mode, NewServer, Role, NewE]};
        Else -> Else
    end.

%%-----------------------------------------------------------------
%% Io:Format debug messages.  Print them as the call-back module sees
%% them, not as the real erlang messages.  Use trace for that.
%%-----------------------------------------------------------------
%% @hidden
print_event(Dev, {in, Msg}, Name) ->
    case Msg of
        {'$gen_call', {From, _Tag}, Call} ->
            io:format(Dev, "*DBG* ~p got local call ~p from ~w~n",
                      [Name, Call, From]);
        {'$leader_call', {From, _Tag}, Call} ->
            io:format(Dev, "*DBG* ~p got global call ~p from ~w~n",
                      [Name, Call, From]);
        {'$gen_cast', Cast} ->
            io:format(Dev, "*DBG* ~p got local cast ~p~n",
                      [Name, Cast]);
        {'$leader_cast', Cast} ->
            io:format(Dev, "*DBG* ~p got global cast ~p~n",
                      [Name, Cast]);
        _ ->
            io:format(Dev, "*DBG* ~p got ~p~n", [Name, Msg])
    end;
print_event(Dev, {out, Msg, To, State}, Name) ->
    io:format(Dev, "*DBG* ~p sent ~p to ~w, new state ~w~n",
              [Name, Msg, To, State]);
print_event(Dev, {noreply, State}, Name) ->
    io:format(Dev, "*DBG* ~p new state ~w~n", [Name, State]);
print_event(Dev, Event, Name) ->
    io:format(Dev, "*DBG* ~p dbg  ~p~n", [Name, Event]).


handle_msg(Msg={'$leader_call', From, Request}, Server=#server{mod=Mod, state=State}, Role=leader, E) ->
    case catch Mod:handle_leader_call(Request, From, State, E) of
        {reply, Reply, NState} ->
            NewServer = reply(From, {leader,reply,Reply},
                              Server#server{state = NState}, Role, E),
            loop(NewServer, Role, E);
        {reply, Reply, Broadcast, NState} ->
            NewE = broadcast({from_leader,Broadcast}, E),
            NewServer = reply(From, {leader,reply,Reply},
                              Server#server{state = NState}, Role,
                              NewE),
            loop(NewServer, Role, NewE);
        {noreply, NState} = Reply ->
            NewServer = handle_debug(Server#server{state = NState},
                                     Role, E, Reply),
            loop(NewServer, Role, E);
        {stop, Reason, Reply, NState} ->
            {'EXIT', R} =
                (catch terminate(Reason, Msg,
                                 Server#server{state = NState},
                                 Role, E)),
            reply(From, Reply),
            exit(R);
        Other ->
            handle_common_reply(Other, Msg, Server, Role, E)
    end;
handle_msg(Msg={'$leader_call', From, Request}, Server=#server{mod=Mod, state=State}, Role, E=#election{mode=local}) ->
    Reply = (catch Mod:handle_leader_call(Request,From,State,E)),
    handle_call_reply(Reply, Msg, Server, Role, E);
%%%    handle_common_reply(Reply, Msg, Server, Role, E);
handle_msg({'$leader_cast', Cast} = Msg,
       #server{mod = Mod, state = State} = Server, Role,
           #election{mode = local} = E) ->
    Reply = (catch Mod:handle_leader_cast(Cast,State,E)),
    handle_common_reply(Reply, Msg, Server, Role, E);
handle_msg({'$leader_cast', Cast} = Msg,
           #server{mod = Mod, state = State} = Server, elected = Role, E) ->
    Reply = (catch Mod:handle_leader_cast(Cast, State, E)),
    handle_common_reply(Reply, Msg, Server, Role, E);
handle_msg({from_leader, Cmd} = Msg,
           #server{mod = Mod, state = State} = Server, Role, E) ->
    handle_common_reply(catch Mod:from_leader(Cmd, State, E),
                        Msg, Server, Role, E);
handle_msg({'$leader_call', From, Request}, Server, Role, E=#election{buffered=Buffered, leader=Leader}) ->
    Ref = make_ref(),
    Leader ! {'$leader_call', {self(), Ref}, Request},
    NewBuffered = [{Ref,From}|Buffered],
    loop(Server, Role, E#election{buffered = NewBuffered});
handle_msg({Ref, {leader,reply,Reply}}, Server, Role,
           #election{buffered = Buffered} = E) ->
    {value, {_,From}} = lists:keysearch(Ref,1,Buffered),
    NewServer = reply(From, {leader,reply,Reply}, Server, Role,
                      E#election{buffered = lists:keydelete(Ref,1,Buffered)}),
    loop(NewServer, Role, E);
handle_msg({'$gen_call', From, Request} = Msg,
           #server{mod = Mod, state = State} = Server, Role, E) ->
    Reply = (catch Mod:handle_call(Request, From, State)),
    handle_call_reply(Reply, Msg, Server, Role, E);
handle_msg({'$gen_cast',Msg} = Cast,
           #server{mod = Mod, state = State} = Server, Role, E) ->
    handle_common_reply(catch Mod:handle_cast(Msg, State),
                        Cast, Server, Role, E);
handle_msg(Msg,
           #server{mod = Mod, state = State} = Server, Role, E) ->
    handle_common_reply(catch Mod:handle_info(Msg, State),
                        Msg, Server, Role, E).


handle_call_reply(CB_reply, {_, From, _Request} = Msg, Server, Role, E) ->
    case CB_reply of
        {reply, Reply, NState} ->
            NewServer = reply(From, Reply,
                              Server#server{state = NState}, Role, E),
            loop(NewServer, Role, E);
        {noreply, NState} = Reply ->
            NewServer = handle_debug(Server#server{state = NState},
                                     Role, E, Reply),
            loop(NewServer, Role, E);
        {activate, _, Reply, NState}
          when E#election.mode == local ->
            reply(From, Reply),
            NServer = Server#server{state = NState},
            begin_election(NServer, E);
        {stop, Reason, Reply, NState} ->
            {'EXIT', R} =
                (catch terminate(Reason, Msg, Server#server{state = NState},
                                 Role, E)),
            reply(From, Reply),
            exit(R);
        Other ->
            handle_common_reply(Other, Msg, Server, Role, E)
    end.


handle_common_reply(Reply, Msg, Server, Role, E) ->
    case Reply of
        {ok, NState} ->
            NewServer = handle_debug(Server#server{state = NState},
                                     Role, E, Reply),
            loop(NewServer, Role, E);
        {ok, Broadcast, NState} ->
            NewE = broadcast({from_leader,Broadcast}, E),
            NewServer = handle_debug(Server#server{state = NState},
                                     Role, E, Reply),
            loop(NewServer, Role, NewE);
        {stop, Reason, NState} ->
            terminate(Reason, Msg, Server#server{state = NState}, Role, E);
        {'EXIT', Reason} ->
            terminate(Reason, Msg, Server, Role, E);
        _ ->
            terminate({bad_return_value, Reply}, Msg, Server, Role, E)
    end.


reply({To, Tag}, Reply, #server{state = State} = Server, Role, E) ->
    reply({To, Tag}, Reply),
    handle_debug(Server, Role, E, {out, Reply, To, State}).


handle_debug(#server{debug = []} = Server, _Role, _E, _Event) ->
    Server;
handle_debug(#server{debug = Debug} = Server, _Role, E, Event) ->
    Debug1 = sys:handle_debug(Debug, {?MODULE, print_event},
                              E#election.name, Event),
    Server#server{debug = Debug1}.

%%% ---------------------------------------------------
%%% Terminate the server.
%%% ---------------------------------------------------

terminate(Reason, Msg, #server{mod = Mod,
                               state = State,
                               debug = Debug}, _Role,
          #election{name = Name}) ->
    case catch Mod:terminate(Reason, State) of
        {'EXIT', R} ->
            error_info(R, Name, Msg, State, Debug),
            exit(R);
        _ ->
            case Reason of
                normal ->
                    exit(normal);
                shutdown ->
                    exit(shutdown);
                _ ->
                    error_info(Reason, Name, Msg, State, Debug),
                    exit(Reason)
            end
    end.

%% Maybe we shouldn't do this?  We have the crash report...
error_info(Reason, Name, Msg, State, Debug) ->
    io:format("** Generic leader ~p terminating \n"
             "** Last message in was ~p~n"
             "** When Server state == ~p~n"
             "** Reason for termination == ~n** ~p~n",
             [Name, Msg, State, Reason]),
    sys:print_log(Debug),
    ok.

%%% ---------------------------------------------------
%%% Misc. functions.
%%% ---------------------------------------------------

opt(Op, [{Op, Value}|_]) ->
    {ok, Value};
opt(Op, [_|Options]) ->
    opt(Op, Options);
opt(_, []) ->
    false.

debug_options(Name, Opts) ->
    case opt(debug, Opts) of
        {ok, Options} -> dbg_options(Name, Options);
        _ -> dbg_options(Name, [])
    end.

dbg_options(Name, []) ->
    Opts =
        case init:get_argument(generic_debug) of
            error ->
                [];
            _ ->
                [log, statistics]
        end,
    dbg_opts(Name, Opts);
dbg_options(Name, Opts) ->
    dbg_opts(Name, Opts).

dbg_opts(Name, Opts) ->
    case catch sys:debug_options(Opts) of
        {'EXIT',_} ->
            io:format("~p: ignoring erroneous debug options - ~p~n",
                      [Name, Opts]),
            [];
        Dbg ->
            Dbg
    end.

%%-----------------------------------------------------------------
%% Status information
%%-----------------------------------------------------------------
%% @hidden
format_status(Opt, StatusData) ->
    [PDict, SysState, Parent, Debug, [_Mode, Server, _Role, E]] = StatusData,
    Header = lists:concat(["Status for generic server ", E#election.name]),
    Log = sys:get_debug(log, Debug, []),
    #server{mod = Mod, state = State} = Server,
    Specific =
        case erlang:function_exported(Mod, format_status, 2) of
            true ->
                case catch apply(Mod, format_status, [Opt, [PDict, State]]) of
                    {'EXIT', _} -> [{data, [{"State", State}]}];
                    Else -> Else
                end;
            _ ->
                [{data, [{"State", State}]}]
        end,
    [{header, Header},
     {data, [{"Status", SysState},
             {"Parent", Parent},
             {"Logged events", Log}]} |
     Specific].

broadcast(Msg, #election{monitored = Monitored} = E) ->
    %% When broadcasting the first time, we broadcast to all candidate nodes,
    %% using broadcast/3. This function is used for subsequent broadcasts,
    %% and we make sure only to broadcast to already known nodes.
    %% It's the responsibility of new nodes to make themselves known through
    %% a wider broadcast.
    ToNodes = [N || {_,N} <- Monitored],
    broadcast(Msg, ToNodes, E).

broadcast(capture, ToNodes, #election{monitored = Monitored} = E) ->
    ToMonitor = [N || N <- ToNodes,
                      not(keylists:member(N,2,Monitored))],
    NewE =
        lists:foldl(fun(Node,Ex) ->
                            Ref = erlang:monitor(
                                    process,{Ex#election.name,Node}),
                            Ex#election{monitored = [{Ref,Node}|
                                                    Ex#election.monitored]}
                    end,E,ToMonitor),
    lists:foreach(
      fun(Node) ->
              {NewE#election.name,Node} !
                  {leader,capture,NewE#election.iteration,node(),self()}
      end,ToNodes),
    NewE;
broadcast({elect,Synch},ToNodes,E) ->
    lists:foreach(
      fun(Node) ->
              {E#election.name,Node} ! {leader,elect,Synch,self()}
      end,ToNodes),
    E;
broadcast({from_leader, Msg}, ToNodes, E) ->
    lists:foreach(
      fun(Node) ->
              {E#election.name,Node} ! {from_leader, Msg}
      end,ToNodes),
    E;
broadcast(add_worker, ToNodes, E) ->
    lists:foreach(
      fun(Node) ->
              {E#election.name,Node} ! {leader, add_worker, self()}
      end,ToNodes),
    E.
