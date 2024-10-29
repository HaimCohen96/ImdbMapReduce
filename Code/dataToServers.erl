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
-module(dataToServers).
-author("Haim").

-export([shuffleSend/0, readfile/1]).

%% Get - None
%% This function shuffle the whole data, split in by number of servers and send each server is own data part
%% Return - number of the servers, Servers list

shuffleSend() ->
  % Initial time to send data to the servers
  Start = os:timestamp(),
   % shuffling the data - random (important for MapREduce)
  CSVfile = [Y || {_,Y} <- lists:sort([{rand:uniform(), N} || N <- parse_csv:main(["../newData.csv"])])],
  Servers = monitorServers(readfile(["servers.txt"]), []),
  NumOfServers = lists:flatlength(Servers),
  RowsNumber = lists:flatlength(CSVfile) - 1,
  

  case NumOfServers>0 of
    true -> sendData(CSVfile, Servers, {2, RowsNumber, ceil(RowsNumber / NumOfServers)}),
      TimeForData = round(timer:now_diff(os:timestamp(), Start) / 1000),
      io:format("~p records sent to ~p total servers in ~p ms.~n", [RowsNumber, NumOfServers, TimeForData]);
    false -> io:format("All servers are down~n")
  end,
  {NumOfServers,Servers}.

%%------------------------------Send Data To Servers------------------------------
sendData(CSV, [ServerIndex | Servers], {Start, NumRows, Jump}) ->
  if
    Start + Jump < NumRows -> NextStart = Start + Jump;
    true -> NextStart = NumRows + 2
  end,

  % Proccess of sender spawning
  spawn(fun() -> jobForServer(lists:sublist(CSV, Start, Jump), ServerIndex) end),
  io:format("Data: Finish to send data to  ~p.~n", [ServerIndex]),
  sendData(CSV, Servers, {NextStart, NumRows, Jump});

%%No servers or end of list
sendData(_CSV, [], {_, _, _}) ->
  ok.


%% Send to each server is own data part
jobForServer(CSV_Partition, ServerNode) ->
  gen_server:cast({serverpid, ServerNode}, {store, CSV_Partition}).

%--------------------------Helping functions--------------------------------

%% This function handle reading file - it read the file sepereated by lines
readfile(FileName) ->
  try
    {ok, Binary} = file:read_file(FileName),
    string:tokens(erlang:binary_to_list(Binary), "\r\n")
  catch
    error: _Error -> {os:system_time(), error, "CSV problem - cant read the file provided"}
  end.


%% Get - all the servers
%%This function monitoring each server in the list in order to check which is alive
%%Return - List of all servers that are alive

monitorServers([S0 | Servers], Nodes) ->
  Node = list_to_atom(S0),
  case net_kernel:connect_node(Node) of
    true -> 
      monitorServers(Servers, [Node | Nodes]);
    false -> monitorServers(Servers, Nodes)
  end;
monitorServers([], Servers) -> Servers.
