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
-module(dataAnalyze).
-author("Haim").

%% API
-export([main/1]).

main([File]) ->
    case parse_file(File) of
        {ok, Rows} ->
            Rows;
        {error, Reason} ->
            io:format("Error parsing CSV: ~p~n", [Reason]),
            halt(1)
    end;
main([]) ->
    usage().

usage() ->
    halt(1).

parse_file(FileName) ->
    %% Read the file with UTF-8 encoding
    case file:read_file(FileName) of
        {ok, Binary} ->
            %% Convert binary to string (list of characters)
            String = unicode:characters_to_list(Binary, utf8),
            {ok, parse(String)};
        {error, Reason} ->
            {error, Reason}
    end.

parse(String) ->
    %% Split the string into lines
    Lines = string:split(String, "\n", all),
    %% Remove empty lines (if any)
    NonEmptyLines = lists:filter(fun(Line) -> Line /= "" end, Lines),
    %% Parse each line
    ParsedLines = [parse_line(Line) || Line <- NonEmptyLines],
    %% Return the parsed lines
    ParsedLines.

parse_line(Line) ->
    {Fields, []} = parse_fields(Line),
    %% Convert fields to tuple without reversing
    list_to_tuple(Fields).

parse_fields(Line) ->
    parse_fields(Line, []).

parse_fields([], Acc) ->
    {lists:reverse(Acc), []};
parse_fields(Line, Acc) ->
    Line1 = skip_whitespace(Line),
    case Line1 of
        [] ->
            {lists:reverse(Acc), []};
        _ ->
            {Field, Rest} = parse_field(Line1),
            parse_fields(Rest, [Field | Acc])
    end.

parse_field([$\" | Rest]) ->
    %% Quoted field
    {FieldChars, RestAfterField} = parse_quoted_field(Rest, []),
    Field = lists:reverse(FieldChars),
    Rest1 = skip_separator(RestAfterField),
    {Field, Rest1};
parse_field(Line) ->
    %% Unquoted field
    {FieldChars, RestAfterField} = parse_unquoted_field(Line, []),
    Field = lists:reverse(FieldChars),
    Rest1 = skip_separator(RestAfterField),
    {Field, Rest1}.

parse_quoted_field([], Acc) ->
    %% Unexpected end of input
    {Acc, []};
parse_quoted_field([$\" | [$\" | Rest]], Acc) ->
    %% Escaped quote
    parse_quoted_field(Rest, [$\" | Acc]);
parse_quoted_field([$\" | Rest], Acc) ->
    %% End of quoted field
    {Acc, Rest};
parse_quoted_field([Char | Rest], Acc) ->
    parse_quoted_field(Rest, [Char | Acc]).

parse_unquoted_field([], Acc) ->
    {Acc, []};
parse_unquoted_field([$, | Rest], Acc) ->
    {Acc, [$, | Rest]};
parse_unquoted_field([Char | Rest], Acc) when Char =/= $\n ->
    parse_unquoted_field(Rest, [Char | Acc]);
parse_unquoted_field(Rest, Acc) ->
    {Acc, Rest}.

skip_whitespace([$\s | Rest]) ->
    skip_whitespace(Rest);
skip_whitespace(Line) ->
    Line.

skip_separator([$, | Rest]) ->
    skip_whitespace(Rest);
skip_separator(Line) ->
    skip_whitespace(Line).