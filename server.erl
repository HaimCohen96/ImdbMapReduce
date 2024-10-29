%%%-------------------------------------------------------------------
%%% @Author: Haim Fellner Cohen
%%% @2024
%%% @Ben Gurion University
%%%
%%% @Functional programming in concurrent and distributed systems
%%% @Dr. Yehuda Ben-Shimol
%%% @end
%%% Created : Oct 2024
%%%-------------------------------------------------------------------
-module(server).
-author("Haim").

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(server_state, {table_name = none}).
-record(movie_data, {id, title, actors}).
-record(query,{type, searchVal, searchCategory, actorsList}).

-define(MOVIE_RECORD, record_info(fields, movie_data)).

%% Spawns the server
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  {ok, Pid} = gen_server:start_link({global, node()}, ?MODULE, [], []),
  register(serverpid, Pid).

%%%-------------------------handle callbacks messages -------------------

-spec(init(Args :: term()) ->
  {ok, State :: #server_state{}} | {ok, State :: #server_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  [MasterNode| _Clients] = readfile(["clients.txt"]),
  gen_server:cast({masterpid, list_to_atom(MasterNode)}, {nodeup, node()}),
  {ok, #server_state{}}.

%%% ----------------------------- handle call messages -----------------------------

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #server_state{}) ->
  {reply, Reply :: term(), NewState :: #server_state{}} |
  {reply, Reply :: term(), NewState :: #server_state{}, timeout() | hibernate} |
  {noreply, NewState :: #server_state{}} |
  {noreply, NewState :: #server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #server_state{}} |
  {stop, Reason :: term(), NewState :: #server_state{}}).

%% Handle_call - test
handle_call(test, _From, State = #server_state{}) ->
  Details = ets:tab2list(State#server_state.table_name),
  {reply, Details, State};

%% Handle_call - regular query 
handle_call(Query = #query{}, {FromPID, _Tag}, State = #server_state{}) ->
  PID = spawn(fun() -> sendToMR(Query, State#server_state.table_name, FromPID) end),
  io:format("Task: Task is recieved from ~p, using PID ~p~n in order to do so",[FromPID,PID]),
  {reply, ok, State};  

%% Handle_call- ignore anything else
handle_call(_Request, _From, State = #server_state{}) ->
  {reply, ok, State}.

%%% ----------------------------- Handle Cast -----------------------------

-spec(handle_cast(Request :: term(), State :: #server_state{}) ->
  {noreply, NewState :: #server_state{}} |
  {noreply, NewState :: #server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #server_state{}}).

%% This phase is handle on data recieving from Master
handle_cast({store, Data}, State = #server_state{}) ->
  N = atom_to_list(node()),
  Node = string:sub_string(N, 1, string:cspan(N, "@")),
  io:format(Node ++ " received data. Saving...~n"),
  Start = os:timestamp(),
  TableFile = saveData(Data),
  io:format("Data: Done recieve and organize data in ~p ms.~n",[round(timer:now_diff(os:timestamp(), Start) / 1000)]),
  {noreply, State#server_state{table_name = TableFile}};
handle_cast(_Request, State = #server_state{}) ->
  {noreply, State}.


%%% ----------------------------- Handle Info -----------------------------

-spec(handle_info(Info :: timeout() | term(), State :: #server_state{}) ->
  {noreply, NewState :: #server_state{}} |
  {noreply, NewState :: #server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #server_state{}}).

handle_info(_Info, State = #server_state{}) ->
  {noreply, State}.

%% Handle terminate
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #server_state{}) -> term()).
terminate(_Reason, _State = #server_state{}) ->
  ok.

%% Handle code change
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #server_state{},
    Extra :: term()) ->
  {ok, NewState :: #server_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State = #server_state{}, _Extra) ->
  {ok, State}.

%%%---------------------Server functions---------------------

%% Save the data into ETS table

saveData(Data) ->
  Table = ets:new(moviesdb, [set, public, named_table, {read_concurrency, true}]),
  dictCrt(Data),
  ets:tab2file(moviesdb, "table_" ++ atom_to_list(node())),
  ets:delete(Table),
  "table_" ++ atom_to_list(node()).

%%Create paris of {key, value} before insertion to ETS
dictCrt([]) ->
  ok;
dictCrt([H | T]) ->
  Id = element(1, H),
  Details = #movie_data{
    id = element(1, H),
    title = element(2, H),
    actors = element(3, H)},
  ets:insert(moviesdb, {Id, Details}),
  dictCrt(T).

%% sCreate procces in order to handle on the mapreduce algorithm
sendToMR(Query = #query{}, Table_name, FromPID) ->
  Start = os:timestamp(),
  Result = mapReduce:get(Table_name, Query),
  ProcessCount = erlang:system_info(process_count),
  io:format("Number of processes: ~p~n", [ProcessCount]),
  CoresUsed = erlang:system_info(schedulers_online),
  io:format("Number of cores being used: ~p~n", [CoresUsed]),
  io:format("Got the answer at ~p in ~p ms, sending it to ~p~n",
    [self(), round(timer:now_diff(os:timestamp(), Start) / 1000),FromPID]),
  FromPID ! Result.

%% readfile - read file as strings separated by lines
readfile(FileName) ->
  {ok, Binary} = file:read_file(FileName),
  string:tokens(erlang:binary_to_list(Binary), "\r\n").