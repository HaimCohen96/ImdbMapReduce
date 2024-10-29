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
-module(actor_graph).
-author("Haim").

-export([create_actor_graph/2, generate_dot_file/2, generate_graph_image/2]).

%% Function to create the actor graph
create_actor_graph(Result, ActorName) ->
    Graph = digraph:new(),

    %% Add root node (Level 0) with the actor's name
    RootNode = digraph:add_vertex(Graph, level0, ActorName),
    process_levels(Result, Graph, RootNode),
    Graph.

%% process each level
process_levels([], _Graph, _ParentNode) ->
    ok;
process_levels([{LevelKey, Actors}|Rest], Graph, ParentNode) ->
    Table = create_table(Actors),
    %% Add vertex for the current level 
    LevelNode = digraph:add_vertex(Graph, LevelKey, Table),
    %% Add edge from parent node to current node
    digraph:add_edge(Graph, ParentNode, LevelNode),
    process_levels(Rest, Graph, LevelNode).

%% Create a tables of count and actor names
create_table(Actors) ->
    Count = length(Actors),
    [Count | Actors].

%% Generate a Graphviz DOT file for actor data
generate_dot_file(Graph, FileName) ->
    {ok, File} = file:open(FileName, [write]),
    io:format(File, "digraph ActorGraph {~n", []),
    io:format(File, "  rankdir=LR;~n", []),
    Vertices = digraph:vertices(Graph),
    lists:foreach(fun(Vertex) ->
        {_, Label} = digraph:vertex(Graph, Vertex),
        LabelStr = format_label(Label),
        EscapedLabelStr = escape_quotes(LabelStr),
        case Vertex of
            level0 ->
                io:format(File, "  \"~p\" [label=\"~s\", shape=ellipse, style=filled, fillcolor=lightblue];~n", [Vertex, EscapedLabelStr]);
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

%% Format labels - helping function
format_label(Label) when is_list(Label) ->
    [Count | ActorNames] = Label,
    Lines = ["Count: " ++ integer_to_list(Count)],
    ActorLines = lists:map(fun format_actor/1, ActorNames),
    FinalLabel = string:join(Lines ++ ActorLines, "\\n"),
    FinalLabel;
format_label(Label) when is_binary(Label) ->
    binary_to_list(Label).

format_actor({actor_data, _ID, Name, _Count}) ->
    Name;
format_actor(Other) ->
    io_lib:format("~p", [Other]).

escape_quotes(LabelStr) ->
    string:replace(LabelStr, "\"", "\\\"", all).

%% Function to generate the DOT file and then execute the 'dot' command to create a PNG image
generate_graph_image(Graph, BaseFileName) ->
    DotFile = BaseFileName ++ ".dot",
    PngFile = BaseFileName ++ ".png",
    generate_dot_file(Graph, DotFile),
    %% Execute the 'dot' command
    Command = "dot -Tpng " ++ DotFile ++ " -o " ++ PngFile,
    io:format("Executing Graphviz command: ~s~n", [Command]),
    GraphvizOutput = os:cmd(Command),
    io:format("Graphviz output: ~s~n", [GraphvizOutput]),
    ok.
