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
-module(clientGUI).
-author("Haim").

-export([start/0, readfile/1]).
-include_lib("wx/include/wx.hrl").

-record(query, {type, searchVal, searchCategory, actorsList}).

start() ->
  
  %%--------------------- Initializing the components------------------------
  Options = ["Title", "Actor"],
  WX = wx:new(),
  Frame = wxFrame:new(WX, ?wxID_ANY, "Final Project - MapReduce"),
  MainSizer = wxStaticBoxSizer:new(?wxVERTICAL, Frame),
  Headline = wxStaticText:new(Frame, ?wxID_ANY, "Insert Movie or Actor:"),
  Headline2 = wxStaticText:new(Frame, ?wxID_ANY, "Select Method:"),
  TextCtrl = wxTextCtrl:new(Frame, ?wxID_ANY, [{value, ""}, {style, ?wxDEFAULT}]),
  TextCtrlValidation = wxStaticText:new(Frame, ?wxID_ANY, ""),
  ButtonSend = wxButton:new(Frame, ?wxID_ANY, [{label, "Send value to search"}, {style, ?wxBU_EXACTFIT}]),
  ListBox = wxListBox:new(Frame, ?wxID_ANY, [{size, {-1, 100}}, {choices, Options}, {style, ?wxLB_SINGLE}]),
  Image = wxImage:new("logo.png", []),
  Bitmap = wxBitmap:new(wxImage:scale(Image, round(wxImage:getWidth(Image) * 0.7), round(wxImage:getHeight(Image) * 0.7), [{quality, ?wxIMAGE_QUALITY_HIGH}])),
  StaticBitmap = wxStaticBitmap:new(Frame, ?wxID_ANY, Bitmap),

  %%--------------------- Setting Properties and visualiztions--------------------
  Font = wxFont:new(25, ?wxDEFAULT, ?wxNORMAL, ?wxBOLD),
  EFont = wxFont:new(18, ?wxDEFAULT, ?wxNORMAL, ?wxBOLD),
  ListBFont = wxFont:new(18, ?wxDEFAULT, ?wxNORMAL, ?wxBOLD),
  ButtonFont = wxFont:new(14, ?wxDEFAULT, ?wxNORMAL, ?wxBOLD),
  wxWindow:setFont(Headline, Font),
  wxWindow:setFont(Headline2, Font),
  wxWindow:setFont(ListBox, ListBFont),
  wxListBox:setToolTip(ListBox, "Choose Method"),
  wxWindow:setMinSize(ListBox, {200, -1}),
  wxWindow:setLabel(Frame, "Final Project - MapReduce"),
  wxWindow:setBackgroundColour(Frame, {255, 255, 255}),
  wxWindow:setSize(Frame, 0, 0, 700, 850),
  wxWindow:setFont(ButtonSend, ButtonFont),
  wxStaticText:setForegroundColour(Headline, {40, 40, 40}),
  wxStaticText:setForegroundColour(Headline2, {40, 40, 40}),
  wxStaticText:setForegroundColour(TextCtrlValidation, ?wxRED),
  wxWindow:setMinSize(ButtonSend, {200, -1}),
  wxButton:setBackgroundColour(ButtonSend, {40, 40, 40}),
  wxButton:setForegroundColour(ButtonSend, {255, 255, 255}),
  wxWindow:setFont(TextCtrlValidation, EFont),

  %%--------------------- Order in the sizer ----------------------------------------------------------------------
  wxSizer:add(MainSizer, StaticBitmap, [{flag, ?wxALL bor ?wxEXPAND}]),
  wxSizer:add(MainSizer, Headline2, [{flag, ?wxALL bor ?wxEXPAND}, {border, 8}]),
  wxSizer:add(MainSizer, ListBox, [{flag, ?wxALIGN_CENTER bor ?wxALL}, {border, 8}]),
  wxSizer:add(MainSizer, Headline, [{flag, ?wxALL bor ?wxEXPAND}, {border, 8}]),
  wxSizer:add(MainSizer, TextCtrl, [{flag, ?wxALL bor ?wxEXPAND}, {border, 8}]),
  wxSizer:add(MainSizer, ButtonSend, [{flag, ?wxALIGN_CENTER bor ?wxALL}, {border, 8}]),
  wxSizer:add(MainSizer, TextCtrlValidation, [{flag, ?wxALIGN_CENTER bor ?wxEXPAND}, {border, 8},{proportion, 1}]),

%% Button click - event:
  wxEvtHandler:connect(ButtonSend, command_button_clicked, [{callback, fun handle_click_event/2},
    {userData, {wx:get_env(), TextCtrl, ListBox, TextCtrlValidation}}]),
  wxWindow:setSizer(Frame, MainSizer),
  wxFrame:show(Frame).

%% Button Function
handle_click_event(A = #wx{}, _B) ->
  {Env, TextBox, ListBox, TextCtrlValidation} = A#wx.userData,
  wx:set_env(Env),
  SelectionValue = wxListBox:getSelection(ListBox),
  TextValue = wxTextCtrl:getValue(TextBox),

  %% Check valid text and method
  IsListBoxNotEmpty = SelectionValue /= -1,
  IsTextBoxNotEmpty = TextValue =/= "",
  IsInputValid = IsListBoxNotEmpty and IsTextBoxNotEmpty,

  case IsInputValid of
    true ->
      wxTextCtrl:setLabel(TextCtrlValidation, "Sending new task to the Master"),
      wxTextCtrl:setForegroundColour(TextCtrlValidation, {255, 200, 0}),
      Query = #query{type = generic, searchVal = wxTextCtrl:getValue(TextBox), searchCategory = wxListBox:getString(ListBox, wxListBox:getSelection(ListBox)), actorsList = []},
      [MasterNode | _T] = readfile(["clients.txt"]),
      StartTime = os:timestamp(),
      try
        gen_server:call({masterpid, list_to_atom(MasterNode)}, Query), %% Receive acknowledgement from the master
        Self = self(),
        register(monitormasterpid, spawn(fun() -> monitorMaster(Self, MasterNode) end)),
        
        %% Updated receive block to handle the result from the master
        receive
          {NumOfServers2, Result} ->  %% Pattern matching the response from the master
              io:format("Received results from ~p servers~n", [NumOfServers2]),
              %io:format("Result: ~p~n", [Result]),  %% Print the result for debugging
              EndTIme = os:timestamp(),
              SearchValue = wxTextCtrl:getValue(TextBox),
              io:format("Result: ~p~n", [SearchValue]),
              SearchValueBinary = list_to_binary(SearchValue),
              RunningTime = round(timer:now_diff(EndTIme, StartTime) / 1000),
              case Query#query.searchCategory of
                "Title" ->
                    %% Handle movie_data using movie_graph module
                    %io:format("Handling 'Title' query with movie_graph module.~n"),
                    Graph = movie_graph:create_movie_graph(Result, SearchValueBinary),
                    movie_graph:generate_graph_image(Graph, "movie_graph"),
                    %% Update validation text
                    wxTextCtrl:setForegroundColour(TextCtrlValidation, {255, 200, 0}),
                    wxTextCtrl:setLabel(TextCtrlValidation, "Movie graph created and exported to 'movie_graph.png'. \n The search time is:" ++ integer_to_list(RunningTime) ++ "ms"),
                    %% Display the generated graph image
                    display_graph_image("movie_graph.png");
                
                "Actor" ->
                    %% Handle actor_data using actor_graph module
                    %io:format("Handling 'Actor' query with actor_graph module.~n"),
                    Graph = actor_graph:create_actor_graph(Result, SearchValueBinary),
                    actor_graph:generate_graph_image(Graph, "actor_graph"),
                    %% Update validation text
                    wxTextCtrl:setForegroundColour(TextCtrlValidation, {255, 200, 0}),
                    wxTextCtrl:setLabel(TextCtrlValidation, "Actor graph created and exported to 'actor_graph.png' \n The search time is:" ++ integer_to_list(RunningTime) ++ "ms"),
                    %% Display the generated graph image
                    display_graph_image("actor_graph.png");
                _ ->
                    %% Handle unexpected search categories
                    io:format("Invalid search category: ~p~n", [Query#query.searchCategory]),
                    wxTextCtrl:setForegroundColour(TextCtrlValidation, ?wxRED),
                    wxTextCtrl:setLabel(TextCtrlValidation, "Invalid search category selected.")
            end;
          masterdown ->
              wxTextCtrl:setForegroundColour(TextCtrlValidation, ?wxRED),
              wxTextCtrl:setLabel(TextCtrlValidation, "System: Master is offline, \n try again later");

          _Other ->
              io:format("Unexpected result received: ~p~n", [_Other]),  %% Print unexpected results for debugging
              wxTextCtrl:setForegroundColour(TextCtrlValidation, ?wxRED),
              wxTextCtrl:setLabel(TextCtrlValidation, "System: Unexpected result, try again")
        end,
        monitormasterpid ! shutdown
      catch
        _: _ -> wxTextCtrl:setForegroundColour(TextCtrlValidation, ?wxRED),
          wxTextCtrl:setLabel(TextCtrlValidation, "System: Master is offline, \n try again later")
      end;
    false ->
      io:format("Bad Input - Try again~n"),
      wxTextCtrl:setForegroundColour(TextCtrlValidation, ?wxRED),
      wxTextCtrl:setLabel(TextCtrlValidation, "System: Bad Input - Try again")
  end.

%% Display the generated graph image using wxFrame
display_graph_image(ImageFile) ->
  %% Debugging: Start displaying image
  io:format("Displaying image: ~p~n", [ImageFile]),

  %% Create a new wxFrame to display the image
  WX = wx:new(),
  Image = wxImage:new(ImageFile, []),
  OrigWidth = wxImage:getWidth(Image),
  OrigHeight = wxImage:getHeight(Image),
  Frame = wxFrame:new(WX, ?wxID_ANY, "Graph Visualization", [{size, {OrigWidth + 100, OrigHeight + 100}}]),

  %% Debugging: Attempt to load the image
  case wxImage:isOk(Image) of
    true ->
      io:format("Image loaded successfully: ~p~n", [ImageFile]),
      
      %% Scale the image and display it
      Bitmap = wxBitmap:new(wxImage:scale(Image, OrigWidth, OrigHeight, [{quality, ?wxIMAGE_QUALITY_HIGH}])),
      StaticBitmap = wxStaticBitmap:new(Frame, ?wxID_ANY, Bitmap),
      
      %% Set up the sizer for layout
      Sizer = wxBoxSizer:new(?wxVERTICAL),
      wxSizer:add(Sizer, StaticBitmap, [{flag, ?wxEXPAND bor ?wxALL}, {border, 10}]),
      
      %% Apply sizer and show the frame
      wxWindow:setSizer(Frame, Sizer),
      wxFrame:show(Frame);
      %io:format("Image displayed successfully.~n");
    false ->
      io:format("System: Failed to load image: ~p~n", [ImageFile])
  end.

%%---------------------Helping functions---------------------

%% Monitor after the master
monitorMaster(FromPID, MasterNode) ->
  gen_server:cast({masterpid, list_to_atom(MasterNode)}, ping),
  net_kernel:monitor_nodes(true),
  monitorMasterReceive(FromPID, MasterNode).

monitorMasterReceive(FromPID, MasterNode) ->
  MasterNodeAtom = list_to_atom(MasterNode),
  receive
    {nodedown, MasterNodeAtom} -> FromPID ! masterdown;
    shutdown -> void;
    _ -> monitorMasterReceive(FromPID, MasterNode)
  end.

readfile(FileName) ->
  try
    {ok, Binary} = file:read_file(FileName),
    string:tokens(erlang:binary_to_list(Binary), "\r\n")
  catch
    error: _Error -> {os:system_time(), error, "System: Cannot find the given master"}
  end.
