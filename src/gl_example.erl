-module(gl_example).

-behaviour(gen_leader).

%% API
-export([start_link/0]).

%% gen_leader callbacks
-export([init/1,
         handle_cast/3,
         handle_call/4,
         handle_info/3,
         handle_leader_call/4,
         handle_leader_cast/3,
         handle_DOWN/3,
         elected/2,
         surrendered/3,
         from_leader/3,
         code_change/4,
         terminate/2]).


-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_leader:start_link(?SERVER, "group_name", ?MODULE, [], []).

%%%===================================================================
%%% gen_leader callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

elected(State, _Election) ->
    io:format("I was elected!~n"),
    {ok, [], State}.

surrendered(State, _Synch, _Eelection) ->
    {ok, State}.

handle_leader_call(_Request, _From, State, _Election) ->
    io:format("_Request ~p~n", [_Request]),
    {reply, ok, State}.

handle_leader_cast(_Request, State, _Election) ->
    {noreply, State}.

from_leader(_Synch, State, _Election) ->
    {ok, State}.

handle_DOWN(_Node, State, _Election) ->
    {ok, State}.

handle_call(_Request, _From, State, _Election) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State, _Election) ->
    {noreply, State}.

handle_info(_Info, State, _Election) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Election, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
