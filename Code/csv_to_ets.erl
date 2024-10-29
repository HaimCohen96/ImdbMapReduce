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

-module(csv_to_ets).
-author("Haim").


-export([start/1, init_ets/0, store_csv_in_ets/2, save_ets/1, load_ets/1, print_ets_content/0]).

% Initialize ETS and process the CSV file 
start(FilePath) ->
    init_ets(),
    store_csv_in_ets(FilePath, 85000).

init_ets() ->
    ets:new(movie_table, [set, public, named_table]).

% Read the CSV file and store the first N rows in ETS
store_csv_in_ets(FilePath, Limit) ->
    {ok, Binary} = file:read_file(FilePath),
    Lines = binary:split(Binary, <<"\n">>, [global]),
    store_lines(Lines, Limit, 0).

% Store lines with a counter
store_lines([], _Limit, _Count) ->
    ok; % No more lines to process
store_lines([Line | Rest], Limit, Count) when Count < Limit ->
    parse_and_store(Line), 
    store_lines(Rest, Limit, Count + 1);
store_lines(_, Limit, Count) when Count >= Limit ->
    ok. % Stop processing after the limit is reached

% Parse each line and store in ETS
parse_and_store(Line) ->
    % Remove any leading/trailing whitespace manually
    TrimmedLine = trim_whitespace(Line),
    % Check if the line is not empty
    if byte_size(TrimmedLine) > 0 ->
        case binary:split(TrimmedLine, <<",">>, [global]) of
            [_MovieID, TitleBin | ActorsBins] ->
                % Handle the case where some actor names may have quotes
                Title = binary_to_list(TitleBin),
                ActorsStr = lists:map(fun(X) -> binary_to_list(X) end, ActorsBins),
                Actors = lists:map(fun(A) -> string:strip(A, both, $") end, ActorsStr),
                ets:insert(movie_table, {Title, Actors});
            _ ->
                % If the line doesn't match the expected structure, skip it
                ok
        end;
    true ->
        ok
    end.

% Save the ETS table to a file
save_ets(FilePath) ->
    case ets:tab2file(movie_table, FilePath) of
        ok -> io:format("Data: ETS table successfully saved to ~p~n", [FilePath]);
        {error, Reason} -> io:format("Failed to save ETS table: ~p~n", [Reason])
    end.

% Load the ETS table from a file
load_ets(FilePath) ->
    case ets:file2tab(FilePath) of
        {ok, _} -> io:format("Data: ETS table successfully loaded from ~p~n", [FilePath]);
        {error, Reason} -> io:format("Failed to load ETS table: ~p~n", [Reason])
    end.

% Trim leading and trailing whitespace
trim_whitespace(Bin) ->
    % Convert binary to list
    List = binary_to_list(Bin),
    TrimmedList = string:strip(List, both, $\s),
    list_to_binary(TrimmedList).

% Print the ETS table content for verification - debug uses
print_ets_content() ->
    TableContents = ets:tab2list(movie_table),
    io:format("ETS Table Content: ~p~n", [TableContents]).
