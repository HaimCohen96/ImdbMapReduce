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
-module(master).
-author("Haim").

-behaviour(gen_server).
-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(master_state, {servers = {0,[]}}).
-record(query, {type, searchVal, searchCategory, actorsList}).


%% Beggining of the server - classic genserver
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  {ok, Pid} = gen_server:start_link({global, node()}, ?MODULE, [], []),
  csv_to_ets:start("../newData.csv"),
  csv_to_ets:save_ets("ets_backup.dat"),
  register(masterpid, Pid).

%%%--------------------Template copied from genserver----------------------
%%%--------------------callbacks----------------------

-spec(init(Args :: term()) ->
  {ok, State :: #master_state{}} | {ok, State :: #master_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  io:format("System: Master online.~n"),
  net_kernel:monitor_nodes(true),
  % Sending data to all available servers
  ServersInfo = dataToServers:shuffleSend(),
  flush(),
  {ok, #master_state{servers = ServersInfo}}.


%%% %%%--------------------Call Messages----------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #master_state{}) ->
  {reply, Reply :: term(), NewState :: #master_state{}} |
  {reply, Reply :: term(), NewState :: #master_state{}, timeout() | hibernate} |
  {noreply, NewState :: #master_state{}} |
  {noreply, NewState :: #master_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #master_state{}} |
  {stop, Reason :: term(), NewState :: #master_state{}}).

%% handle_call - task format answer
handle_call(Query = #query{}, {FromPID, _Tag}, State = #master_state{}) ->
  PID = spawn(fun() -> taskStart(Query, FromPID, element(1,State#master_state.servers),element(2,State#master_state.servers)) end),
  io:format("Task: Received new task from ~p , PID ~p created to handle this ~n", [FromPID, PID]),
  {reply, ok, State};

%% Handle_call - do not reaply to other tasks
handle_call(_Request, _From, State = #master_state{}) ->
  {reply, ok, State}.


%%% -----------------------------Cast Messages-----------------------------
-spec(handle_cast(Request :: term(), State :: #master_state{}) ->
  {noreply, NewState :: #master_state{}} |
  {noreply, NewState :: #master_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #master_state{}}).

%% Handle_cast - nodeup format answer
handle_cast({nodeup, Node}, State = #master_state{}) ->
  case lists:member(atom_to_list(Node), dataToServers:readfile(["servers.txt"])) of
    true -> 
      %When sending data to server - no requests available to the master
      %% When node is up - should send all data again (number of servers are changed)
      io:format("System: New node detected: ~p~n", [Node]),
      ServersInfo = dataToServers:shuffleSend(),
      flush(),
      UpdatedState = State#master_state{servers = ServersInfo};
    false ->  UpdatedState = State         
  end,
  {noreply, UpdatedState};

%% Handle_cast - all other casted messages
handle_cast(_Request, State = #master_state{}) ->
  io:format("System: Message received: ~p~n", [_Request]),
  {noreply, State}.


%%% ----------------------------- Handle Info -----------------------------
%% handle info messages (no calls/cast messages)
-spec(handle_info(Info :: timeout() | term(), State :: #master_state{}) ->
  {noreply, NewState :: #master_state{}} |
  {noreply, NewState :: #master_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #master_state{}}).

%% When node is down - should send all data again (number of servers are changed)
handle_info({nodedown, Node}, State = #master_state{}) ->
  case string:str(atom_to_list(Node), "server") > 0 of
    true ->
      io:format("System: A node is down: ~p~n", [Node]),
      ServersInfo = dataToServers:shuffleSend(),
      flush();
  false ->
      ServersInfo = State#master_state.servers
  end,
  {noreply, State#master_state{servers = ServersInfo}};

%% other requests
handle_info(_Info, State = #master_state{}) ->
  io:format("System: Message received: ~p~n", [_Info]),
  {noreply, State}.

%% Terminate function
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #master_state{}) -> term()).
terminate(_Reason, _State = #master_state{}) ->
  ok.

%% Handle code change
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #master_state{},
    Extra :: term()) ->
  {ok, NewState :: #master_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State = #master_state{}, _Extra) ->
  {ok, State}.

%%%---------------------Master code functions---------------------
%%%
%% Send the task from client to avaiavle servers- waiting for result and sending to the client
taskStart(Query = #query{type = generic, searchCategory = _Any, searchVal = MovieTitle, actorsList = []}, FromPID, NumOfServers, Servers) ->
  % Load the ETS table from the backup file
  case ets:file2tab("ets_backup.dat") of
      {ok, _} ->
          io:format("Data: ETS table successfully loaded.~n");
      {error, Reason} ->
          io:format("Data: Failed to load ETS table: ~p~n", [Reason]),
          exit(Reason)
  end,

  % Search for the actors based on the movie title
  Actors =
  case ets:lookup(movie_table, MovieTitle) of
      [{_Title, FoundActors}] ->
          FoundActors;  % Return Actors
      [] ->
          []  % Return an empty list if no actors are found
  end,

  % Create a new Query with the updated actorsList
  UpdatedQuery = Query#query{actorsList = Actors},
  
  % Continue the sendQuery process with the updated query
  Result = sendTask(UpdatedQuery, Servers, NumOfServers),

  io:format("Task: Received result at ~p, starting to organize it.~n", [self()]),
  % Merge the results from different servers
  FinalResult = merge_results(Result),

  %% Process the FinalResult to handle actor duplicates and level updates
  ProcessedFinalResult =
      case determine_and_process(FinalResult) of
          {process, NewResult} ->
              NewResult;  % Actors were processed
          {skip, FinalResult} ->
              % Movie data detected, sort levels alphabetically
              sort_levels(FinalResult);
          {empty, FinalResult} ->
              FinalResult;  % All levels empty, no processing needed
            _->
              []
      end,

  NumOfServers2 = lists:flatlength(monitorServers(Servers, [])),

  % Print the result to the console before sending it
  io:format("Task: Sending the result to process ~p~n Total number of servers who took a part of the task is: ~p~n", [FromPID, NumOfServers2]),
  FromPID ! {NumOfServers2, ProcessedFinalResult}.


%% Beggining - Send to all servers
sendTask(_Query = #query{}, [], NumberOfServers) ->
  gen_server:cast({masterpid, node()}, ping), %% TODO: check if it can be deleted
  net_kernel:monitor_nodes(true),
  gather(NumberOfServers);

%% Middle - send each server a job by pid
sendTask(Query = #query{}, [Server0 | T], _NumberOfServers) ->
  Self = self(),
  PID = spawn(fun() -> jobForServer(Self, Query, Server0) end),
  io:format("Entered sendQuery function and ~p spawned process ~p for server ~p~n", [self(), PID, Server0]),
  sendTask(Query, T, _NumberOfServers).

%% keep the answer from each pid - the answers are send to parents
jobForServer(ParentPID, Query = #query{}, Server) ->
  % sending from gen_server the values of the data
  gen_server:call({serverpid, Server}, Query),
  receive
    table_error ->
      io:format("Error:table_error message recieved from server : ~p ~n", [Server]);
    Reply2 ->
      ParentPID ! Reply2
  end.

% Funtion in order to handle recieve from servers - wait to recieve all data
gather(0) -> [];
gather(Results) ->
  io:format("Entered gather function expecting ~p results ~n", [Results]),
  receive
    {nodedown, Server} ->
      case string:str(atom_to_list(Server), "server") > 0 of
        true -> % Emergency - send whatever you got
          io:format("System: A server is down, Sending whatever data it has~n"),
          gather(Results-1);
        false -> % Not in list - dont care
          gather(Results)
      end;
    {nodeup, _} ->    %% ignore anything else
      gather(Results);
    Result ->
      io:format("Received result at ~p~n", [self()]),
      Result ++ gather(Results - 1)
  end.
%%-----------------------Functions to organize recieved data-----------------------

%% merge_results/1 - Merges entries of the same levels together
merge_results(Result) ->
    % Use a map to accumulate data for each level
    Acc0 = #{},
    Acc = lists:foldl(fun({Level, MovieDataList}, AccIn) ->
                          % Get existing list for the level or an empty list
                          ExistingList = maps:get(Level, AccIn, []),
                          % Append the new movie data to the existing list
                          NewList = ExistingList ++ MovieDataList,
                          % Update the map with the new list
                          maps:put(Level, NewList, AccIn)
                      end, Acc0, Result),
    % Convert the map back to a list of {Level, MovieDataList} tuples
    maps:to_list(Acc).

%% Function to determine the first non-empty level and process accordingly
determine_and_process(FinalResult) ->
    Levels = FinalResult,
    determine_and_process_levels(Levels, FinalResult).

%% Recursive function to check each level
determine_and_process_levels([{level1, Level1List} | _RestLevels], FinalResult) when Level1List =/= [] ->
    process_level(Level1List, FinalResult);
determine_and_process_levels([{level1, []}, {level2, Level2List} | _RestLevels], FinalResult) when Level2List =/= [] ->
    process_level(Level2List, FinalResult);
determine_and_process_levels([{level1, []}, {level2, []}, {level3, Level3List} | _RestLevels], FinalResult) when Level3List =/= [] ->
    process_level(Level3List, FinalResult);
determine_and_process_levels([{level1, []}, {level2, []}, {level3, []}, {level4, Level4List} | _], FinalResult) when Level4List =/= [] ->
    process_level(Level4List, FinalResult);
determine_and_process_levels([_ | RestLevels], FinalResult) ->
    determine_and_process_levels(RestLevels, FinalResult);
determine_and_process_levels([], _FinalResult) ->
    {empty, []}.

%% Function to process a single level's list
%% Check if its Title, Actor or something else.
process_level(LevelList, FinalResult) ->
    case LevelList of
        [{actor_data, 0, _Name, _Level} | _] ->
            %% If actor_data, process actors
            {process, process_actors(FinalResult)};
        [{movie_data, 0, _Name, _Level} | _] ->
            %% If movie_data, skip processing actors
            {skip, FinalResult};
        _ ->
            %% Unknown data type, skip processing
            {skip, FinalResult}
    end.

%% Function to process the merged actor data
process_actors(MergedData) ->
    %% Flat all actor lists into a single list
    AllActors = lists:foldl(fun({_Level, ActorList}, Acc) ->
                                  Acc ++ ActorList
                              end, [], MergedData),
    
    %% Find and count occurrences of each actor by level
    ActorLevelCounts = count_occurrences(AllActors),    
    %% Calculate total counts for each actor
    TotalCounts = calculate_total_counts(ActorLevelCounts),    
    %% Update the levels based on total counts
    ActorsWithNewLevels = [ {actor_data, 0, Name, determine_level(TotalCount)}
                           || {Name, TotalCount} <- maps:to_list(TotalCounts) ],    
    %% Group actors by their new levels
    GroupedActors = group_by_level(ActorsWithNewLevels),    
    %% Double check - all levels from level1 to level4 are present
    [
        {level1, maps:get(level1, GroupedActors, [])},
        {level2, maps:get(level2, GroupedActors, [])},
        {level3, maps:get(level3, GroupedActors, [])},
        {level4, maps:get(level4, GroupedActors, [])}
    ].

%% Count actor occurrences by their levels
count_occurrences(Actors) ->
    lists:foldl(fun({actor_data, 0, Name, Level}, Acc) ->
                    ActorLevels = maps:get(Name, Acc, #{}),
                    NewLevelCount = maps:get(Level, ActorLevels, 0) + 1,
                    NewActorLevels = maps:put(Level, NewLevelCount, ActorLevels),
                    maps:put(Name, NewActorLevels, Acc)
                end, #{}, Actors).

%% Calculate total counts for each actor
calculate_total_counts(ActorLevelCounts) ->
    maps:map(fun(_Name, LevelCounts) ->
                 calculate_total_count(LevelCounts)
             end, ActorLevelCounts).

calculate_total_count(LevelCounts) ->
    lists:foldl(fun({Level, Count}, Total) ->
                    Total + (Level * Count)
                end, 0, maps:to_list(LevelCounts)).

%% Determine the new level based on total count
determine_level(TotalCount) when TotalCount >= 4 ->
    4;
determine_level(TotalCount) ->
    TotalCount.

%% Group actors by their new levels
group_by_level(Actors) ->
    lists:foldl(fun({actor_data, 0, Name, Level}, Acc) ->
                      LevelKey = list_to_atom("level" ++ integer_to_list(Level)),
                      UpdatedList = maps:get(LevelKey, Acc, []) ++ [{actor_data, 0, Name, Level}],
                      maps:put(LevelKey, UpdatedList, Acc)
                  end, #{}, Actors).

%% Sort each level's list alphabetically by name 
sort_levels(ProcessedFinalResult) ->
    [ {Level, sort_level_data(Data)}
      || {Level, Data} <- ProcessedFinalResult ].

%% Sort a level's data list by name
sort_level_data(Data) ->
    lists:sort(fun compare_data/2, Data).

%% Comparison function for sorting data by name
compare_data({actor_data, _, Name1, _}, {actor_data, _, Name2, _}) ->
    Name1 =< Name2;
compare_data({movie_data, _, Name1, _}, {movie_data, _, Name2, _}) ->
    Name1 =< Name2;
compare_data(_, _) ->
    true.

%-------------------Function seen in lessons (Based on)-----------------------

monitorServers([S0 | Servers], Nodes) ->
  case net_kernel:connect_node(S0) of
    true -> 
      monitorServers(Servers, [S0 | Nodes]);
    false -> monitorServers(Servers, Nodes)
  end;
monitorServers([], Servers) -> Servers.

% flush 
flush() ->
  receive
    _ -> flush()
  after
    0 -> ok
  end.
