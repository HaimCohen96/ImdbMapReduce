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

%% movie_graph.erl
-module(movie_graph).
-author("Haim").

-export([create_movie_graph/2, generate_dot_file/2, generate_graph_image/2]).

%% Create the movie graph
create_movie_graph(Result, MovieName) ->
    Graph = digraph:new(),
    RootNode = digraph:add_vertex(Graph, level0, MovieName),
    process_levels(Result, Graph, RootNode),
    Graph.

%% Process each level
process_levels([], _Graph, _ParentNode) ->
    ok;
process_levels([{LevelKey, Movies}|Rest], Graph, ParentNode) ->
    
    %% Creating table for current level
    Table = create_table(Movies),
    LevelNode = digraph:add_vertex(Graph, LevelKey, Table),
    digraph:add_edge(Graph, ParentNode, LevelNode),
    process_levels(Rest, Graph, LevelNode).

%% Create a table with count and movie titles
create_table(Movies) ->
    Count = length(Movies),
    [Count | Movies].

%% Generate a Graphviz DOT file
generate_dot_file(Graph, FileName) ->
    {ok, File} = file:open(FileName, [write]),
    io:format(File, "digraph MovieGraph {~n", []),
    io:format(File, "  rankdir=LR;~n", []),
    %% Write nodes
    Vertices = digraph:vertices(Graph),
    lists:foreach(fun(Vertex) ->
        {_, Label} = digraph:vertex(Graph, Vertex),
        LabelStr = format_label(Label),
        EscapedLabelStr = re:replace(LabelStr, "\"", "\\\"", [global, {return, list}]),
        case Vertex of
            level0 ->
                io:format(File, "  \"~p\" [label=\"~s\", shape=box, style=filled, fillcolor=lightblue];~n", [Vertex, EscapedLabelStr]);
            _ ->
                io:format(File, "  \"~p\" [label=\"~s\", shape=box, style=filled, fillcolor=lightgrey];~n", [Vertex, EscapedLabelStr])
        end
    end, Vertices),
    Edges = digraph:edges(Graph),
    lists:foreach(fun(Edge) ->
        {_, From, To, _} = digraph:edge(Graph, Edge),
        io:format(File, "  \"~p\" -> \"~p\";~n", [From, To])
    end, Edges),
    io:format(File, "}~n", []),
    file:close(File),
    ok.

%% Format labels
format_label(Label) when is_list(Label) ->
    %% Assume Label is a table (list)
    [Count | Movies] = Label,
    Lines = ["Count: " ++ integer_to_list(Count)],
    MovieLines = lists:map(fun format_movie/1, Movies),
    FinalLabel = string:join(Lines ++ MovieLines, "\\n"),
    FinalLabel;
format_label(Label) when is_binary(Label) ->
    binary_to_list(Label).

%% Format movie titles
format_movie({movie_data, _ID, Title, _Directors}) ->
    Title;
format_movie(Other) ->
    io_lib:format("~p", [Other]).

%% Generate the DOT file and then execute the 'dot' command to create a PNG image
generate_graph_image(Graph, BaseFileName) ->
    DotFile = BaseFileName ++ ".dot",
    PngFile = BaseFileName ++ ".png",
    generate_dot_file(Graph, DotFile),
    %% Execute the 'dot' command
    Command = io_lib:format("dot -Tpng ~s -o ~s", [DotFile, PngFile]),
    CommandStr = lists:flatten(Command),
    os:cmd(CommandStr).
