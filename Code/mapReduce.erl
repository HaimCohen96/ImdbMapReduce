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
-module(mapReduce).
-author("Haim").

-record(movie_data, {id, title, actors}).
-record(query, {type, searchVal, searchCategory, actorsList}).
-record(actor_data, {id = 0, name, shared_movies = 0}).

-export([get/2]).

%% Return the values according to pattern matching
get(TableName, Query = #query{searchVal = SearchTitle, searchCategory = "Title", actorsList = ActorsList}) ->
  io:format("Processing map-reduce query with actorsList: ~p~n", [ActorsList]),
  case ets:file2tab(TableName) of
    {ok, Table} ->
      %% Use map_by_actors function directly with actorsList
      Ans = map_by_actors(Table, SearchTitle, ActorsList),
      ets:delete(Table),
      reduce(Ans, Query);
    _ -> table_error
  end;
get(TableName, Query = #query{searchVal = _SearchTitle, searchCategory = "Actor", actorsList = []}) ->
  io:format("Processing map-reduce query without actorsList~n"),
  case ets:file2tab(TableName) of
    {ok, Table} ->
      %% Perform the standard map-reduce operation using movie title
      Ans = map(Table, Query),
      io:format("Actors print: ~p~n",[Ans]),
      ets:delete(Table),
      reduce(Ans, Query);
    _ -> table_error
  end.

%% map_by_actors - performs the map step using actorsList directly
map_by_actors(Table, SearchTitle, ActorsList) ->
    TrimmedActorsList = lists:map(fun trim_whitespace/1, ActorsList),
    Movies = ets:tab2list(Table),

    %% Define a filter function to exclude the SearchTitle movie
    ExcludeMovie = fun(Movie) -> string:equal(Movie#movie_data.title, SearchTitle) =:= false end,

    %% Level 4: Movies with at least 4 common actors
    Level4 = [OtherMovie || {_id, OtherMovie} <- Movies,
              ExcludeMovie(OtherMovie),
              count_common_actors(TrimmedActorsList, tokenize_actors(OtherMovie#movie_data.actors)) >= 4],

    %% Level 3: Movies with at least 3 common actors, excluding those already in Level 4
    Level3 = [OtherMovie || {_id, OtherMovie} <- Movies,
              ExcludeMovie(OtherMovie),
              count_common_actors(TrimmedActorsList, tokenize_actors(OtherMovie#movie_data.actors)) == 3,
              not lists:member(OtherMovie, Level4)],

    %% Level 2: Movies with at least 2 common actors, excluding those already in Level 3 and Level 4
    Level2 = [OtherMovie || {_id, OtherMovie} <- Movies,
              ExcludeMovie(OtherMovie),
              count_common_actors(TrimmedActorsList, tokenize_actors(OtherMovie#movie_data.actors)) == 2,
              not lists:member(OtherMovie, Level3) andalso not lists:member(OtherMovie, Level4)],

    %% Level 1: Movies with at least 1 common actor, excluding those already in Level 2, Level 3, and Level 4
    Level1 = [OtherMovie || {_id, OtherMovie} <- Movies,
              ExcludeMovie(OtherMovie),
              count_common_actors(TrimmedActorsList, tokenize_actors(OtherMovie#movie_data.actors)) == 1,
              not lists:member(OtherMovie, Level2) andalso not lists:member(OtherMovie, Level3) andalso not lists:member(OtherMovie, Level4)],

    {Level1, Level2, Level3, Level4}.

map(Table, #query{type = generic, searchVal = ActorName, searchCategory = "Actor", actorsList = []}) ->    
    %% Retrieve all movies from the ETS table
    Movies = ets:tab2list(Table),    
    %% Find all movies that include the ActorName
    MoviesWithActor = [Movie || {_id, Movie} <- Movies, 
                        lists:member(ActorName, tokenize_actors(Movie#movie_data.actors))],
    CoActors = [Actor || Movie <- MoviesWithActor, Actor <- tokenize_actors(Movie#movie_data.actors)],
    %% Remove the original actor from the co-actors list
    CoActorsFiltered = [A || A <- CoActors, A /= ActorName],    
    %% Count the number of appearances for each co-actor
    CountsMap = lists:foldl(
        fun(A, Acc) -> maps:update_with(A, fun(X) -> X + 1 end, 1, Acc) end,
        #{},
        CoActorsFiltered
    ),
    
    %% Group co-actors by their counts into levels
    {Level1List, Level2List, Level3List, Level4List} = group_coactors_by_level(CountsMap),
    
    io:format("Level1: ~p, Level2: ~p, Level3: ~p, Level4+: ~p~n", 
              [length(Level1List), length(Level2List), length(Level3List), length(Level4List)]),
    
    %% Format co-actors for each level using actor_data records
    FormattedLevel1 = [#actor_data{name = CoActor, shared_movies = 1} || CoActor <- Level1List],
    FormattedLevel2 = [#actor_data{name = CoActor, shared_movies = 2} || CoActor <- Level2List],
    FormattedLevel3 = [#actor_data{name = CoActor, shared_movies = 3} || CoActor <- Level3List],
    FormattedLevel4 = [#actor_data{name = CoActor, shared_movies = C} || 
                       {CoActor, C} <- maps:to_list(CountsMap), C >= 4],
    
    {FormattedLevel1, FormattedLevel2, FormattedLevel3, FormattedLevel4}.


%% Helper function to group co-actors into levels
group_coactors_by_level(CountsMap) ->
    Level1 = [A || {A, C} <- maps:to_list(CountsMap), C =:= 1],
    Level2 = [A || {A, C} <- maps:to_list(CountsMap), C =:= 2],
    Level3 = [A || {A, C} <- maps:to_list(CountsMap), C =:= 3],
    Level4 = [A || {A, C} <- maps:to_list(CountsMap), C >= 4],
    {Level1, Level2, Level3, Level4}.

%% reduce - performs the reduce step based on search category
reduce({Level1, Level2, Level3, Level4}, _Query = #query{searchCategory = "Actor"}) ->
    % Return results per level for Actor search
    [{level1, Level1},
     {level2, Level2},
     {level3, Level3},
     {level4, Level4}];

reduce({Level1, Level2, Level3, Level4}, _Query = #query{searchCategory = "Title"}) ->
    % Return results per level for Title search
    [{level1, format_movies(Level1)},
     {level2, format_movies(Level2)},
     {level3, format_movies(Level3)},
     {level4, format_movies(Level4)}];
    
reduce(_, _) ->
    % Default case
    [].


% Helper function to format movies
format_movies(Movies) ->
    [#movie_data{id = Movie#movie_data.id, title = Movie#movie_data.title, actors = Movie#movie_data.actors} || Movie <- Movies].

tokenize_actors(ActorsString) ->
    RawTokens = string:split(ActorsString, ",", all),
    TrimmedTokens = lists:map(fun trim_whitespace/1, RawTokens),
    TrimmedTokens.

%% Helper function to trim all types of whitespace from both ends
trim_whitespace(String) ->
    % remove leading and trailing whitespace, including Unicode spaces
    re:replace(String, "^[[:space:]]+|[[:space:]]+$", "", [{return, list}, unicode]).

count_common_actors(Actors1, Actors2) ->
    CommonActors = [A || A <- Actors1, lists:member(A, Actors2)],
    CommonCount = length(CommonActors),
    CommonCount.

