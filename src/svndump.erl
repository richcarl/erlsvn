%% =====================================================================
%% This library is free software; you can redistribute it and/or modify
%% it under the terms of the GNU Lesser General Public License as
%% published by the Free Software Foundation; either version 2 of the
%% License, or (at your option) any later version.
%%
%% This library is distributed in the hope that it will be useful, but
%% WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
%% Lesser General Public License for more details.
%%
%% You should have received a copy of the GNU Lesser General Public
%% License along with this library; if not, write to the Free Software
%% Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
%% USA
%%
%% @author Richard Carlsson <richardc@klarna.com>
%% @copyright 2010 Richard Carlsson
%% @doc Functions for working with SVN dumpfiles.

-module(svndump).

-behaviour(gen_server).

%% API
-export([filter/3, fold/3, to_terms/1]).
-export([scan_records/1, header_vsn/1, header_default/1, header_type/1,
	 header_name/1, format_records/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {file}).


-include_lib("eunit/include/eunit.hrl").

-include("../include/svndump.hrl").

-define(CHUNK_SIZE, (1024*1024)).

%% =====================================================================
%% API
%% =====================================================================
%% ---------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%% ---------------------------------------------------------------------
start_link(File) ->
    gen_server:start_link(?MODULE, [File], []).

%% =====================================================================
%% gen_server callbacks
%% =====================================================================

%% ---------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%% ---------------------------------------------------------------------
init([File]) ->
    {ok, FD} = file:open(File, [read,raw,binary]),
    put(eof, false),
    put(file, FD),
    {ok, #state{file=FD}}.

%% ---------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%% ---------------------------------------------------------------------
handle_call({apply, Fun}, _From, State) ->
    try
        Reply = Fun(),
        {reply, Reply, State}
    catch C:T ->
            file:close(State#state.file),
            error_logger:format("last revision: ~p\n"
                                "exception: ~p:~p\n"
                                "stack trace: ~p\n",
                                [get(revision_number), C, T,
                                 erlang:get_stacktrace()]),
            {stop, error, State}
    end.

%% ---------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%% ---------------------------------------------------------------------
handle_cast(stop, State) ->
    {stop, normal, State}.

%% ---------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%% ---------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%% ---------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% ---------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%% ---------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%% ---------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------
%%% Internal functions
%% ---------------------------------------------------------------------


open_outfile(File) ->
    {ok, FD} = file:open(File, [write]),
    FD.

with_infile(File, Fun) ->
    {ok, Pid} = start_link(File),
    Result = gen_server:call(Pid, {apply, Fun}, infinity),
    gen_server:cast(Pid, stop),
    Result.

with_more(Rest, Fun) ->          
    case get(eof) of
        false ->
            More = read_more(),
            Fun(<<Rest/bytes, More/bytes>>);
        EOF when EOF =:= true ; EOF =:= undefined ->
            Fun(eof)
    end.

read_more() ->
    case file:read(get(file), ?CHUNK_SIZE) of
        {ok, Data} when is_binary(Data) ->
            Data;
        eof ->
            put(eof, true),
            <<>>
    end.


%% File format versions:
%%  1 - svn pre-0.18.0
%%  2 - svn 0.18.0
%%  3 - svn 1.1.0
%% 
%% Format:
%%  * SVN-fs-dump-format-version: N\n
%%  * <EMPTY LINE>
%%  * UUID: 7bf7a5ef-cabf-0310-b7d4-93df341afa7e
%%  * <EMPTY LINE>
%%  * any number of revision records:
%%    + Revision-number: N
%%    + Prop-content-length: P
%%    + Content-length: L
%%    + <EMPTY LINE>
%%      - P bytes of property data, ending with "PROPS-END\n"
%%    + <EMPTY LINE>
%%    + any number of node change records:
%%      - Node-path: absolute/path/to/node/in/filesystem
%%      - Node-kind: file | dir  (optional for a delete action)
%%      - Node-action: change | add | delete | replace
%%      - [Node-copyfrom-rev: X]
%%      - [Node-copyfrom-path: path ]
%%      - [Text-copy-source-md5: blob] (checksum of source of copy)
%%      - [Text-content-md5: blob]
%%      - [Text-content-length: T] (missing before svn 0.14.0)
%%      - [Prop-content-length: P] (missing before svn 0.14.0)
%%      - Content-length: Y (always equals P+T)
%%      - <EMPTY LINE>
%%        * Y=P+T bytes of content data (P bytes of property data ending with
%%          "PROPS-END\n" and followed by T bytes of text data)
%%      - <EMPTY LINE>
%%
%% Property data:
%% * any number of key/value entries:
%%   - K N\n
%%     N bytes of data\n
%%     V M\n
%%     M bytes of data\n
%% * PROPS-END\n
%% 
%% Notes:
%% * Content-length is always the last header before the blank line and content.
%% * Content-length should always be included, but is sometimes omitted,
%%   notably in delete-nodes written by the standard svndump.
%% * For directory nodes, the content is only property data, no text data.

scan_header(<<"Content-length: ", B/bytes>>) ->
    {content_length, scan_integer(B)}; % length of following content
scan_header(<<"Node-path: ", B/bytes>>) ->
    {node_path, B}; % path of node
scan_header(<<"Node-kind: ", B/bytes>>) ->
    {node_kind, scan_node_kind(B)}; % kind of node
scan_header(<<"Node-action: ", B/bytes>>) ->
    {node_action, scan_node_action(B)}; % node action type
scan_header(<<"Node-copyfrom-rev: ", B/bytes>>) ->
    {node_copyfrom_rev, scan_integer(B)}; % copied from revision
scan_header(<<"Node-copyfrom-path: ", B/bytes>>) ->
    {node_copyfrom_path, B}; % copied from path
scan_header(<<"Text-content-length: ", B/bytes>>) ->
    {text_content_length, scan_integer(B)}; % length of text content
scan_header(<<"Prop-content-length: ", B/bytes>>) ->
    {prop_content_length, scan_integer(B)}; % length of property content
scan_header(<<"Revision-number: ", B/bytes>>) ->
    {revision_number, scan_integer(B)}; % number of revision record
scan_header(<<"Text-delta: ", B/bytes>>) ->
    {text_delta, scan_bool(B)}; % contents treated as delta
scan_header(<<"Prop-delta: ", B/bytes>>) ->
    {prop_delta, scan_bool(B)}; % contents treated as delta
scan_header(<<"Text-delta-base-md5: ", B/bytes>>) ->
    {text_delta_base_md5, B}; % checksum of base for delta
scan_header(<<"Text-delta-base-sha1: ", B/bytes>>) ->
    {text_delta_base_sha1, B}; % written by svn 1.6; not used
scan_header(<<"Text-copy-source-md5: ", B/bytes>>) ->
    {text_copy_source_md5, B}; % checksum of source of copy
scan_header(<<"Text-copy-source-sha1: ", B/bytes>>) ->
    {text_copy_source_sha1, B}; % written by svn 1.6; not used
scan_header(<<"Text-content-md5: ", B/bytes>>) ->
    {text_content_md5, B}; % MD5 checksup of content
scan_header(<<"Text-content-sha1: ", B/bytes>>) ->
    {text_content_sha1, B}; % written by svn 1.6; not used
scan_header(<<"UUID: ", B/bytes>>) ->
    {uuid, B}; % originating repository
scan_header(<<"SVN-fs-dump-format-version: ", B/bytes>>) ->
    {svn_fs_dump_format_version, scan_integer(B)}; % file version
scan_header(Line) ->
    throw({unknown_header, Line}).

scan_bool(<<"true">>) -> true;
scan_bool(<<"false">>) -> false.

scan_node_kind(<<"file">>) -> file;
scan_node_kind(<<"dir">>) -> dir.

scan_node_action(<<"change">>) -> change;
scan_node_action(<<"add">>) -> add;
scan_node_action(<<"delete">>) -> delete;
scan_node_action(<<"replace">>) -> replace.

scan_integer(B) -> list_to_integer(binary_to_list(B)).
    
%% @doc Yields the minimum svndump version for a header.
header_vsn(svn_fs_dump_format_version) -> 1;
header_vsn(content_length) -> 1;
header_vsn(node_path) -> 1;
header_vsn(node_kind) -> 1;
header_vsn(node_action) -> 1;
header_vsn(node_copyfrom_rev) -> 1;
header_vsn(node_copyfrom_path) -> 1;
header_vsn(text_content_length) -> 1;
header_vsn(prop_content_length) -> 1;
header_vsn(revision_number) -> 1;
header_vsn(text_copy_source_md5) -> 1;
header_vsn(text_content_md5) -> 1;
header_vsn(uuid) -> 2;
header_vsn(text_delta) -> 3;
header_vsn(prop_delta) -> 3;
header_vsn(text_delta_base_md5) -> 3;
header_vsn(text_delta_base_sha1) -> 3;
header_vsn(text_copy_source_sha1) -> 3;
header_vsn(text_content_sha1) -> 3.

%% @doc Yields the default value for a header.
header_default(svn_fs_dump_format_version) -> 0;
header_default(content_length) -> 0;
header_default(node_path) -> <<>>;
header_default(node_kind) -> file;
header_default(node_action) -> add;
header_default(node_copyfrom_rev) -> 0;
header_default(node_copyfrom_path) -> <<>>;
header_default(text_content_length) -> 0;
header_default(prop_content_length) -> 0;
header_default(revision_number) -> 0;
header_default(text_copy_source_md5) -> <<>>;
header_default(text_content_md5) -> <<>>;
header_default(uuid) -> <<>>;
header_default(text_delta) -> false;
header_default(prop_delta) -> false;
header_default(text_delta_base_md5) -> <<>>;
header_default(text_delta_base_sha1) -> <<>>;
header_default(text_copy_source_sha1) -> <<>>;
header_default(text_content_sha1) -> <<>>.

%% @doc Yields the type of value for a header.
header_type(svn_fs_dump_format_version) -> integer;
header_type(content_length) -> integer;
header_type(node_path) -> binary;
header_type(node_kind) -> atom;
header_type(node_action) -> atom;
header_type(node_copyfrom_rev) -> integer;
header_type(node_copyfrom_path) -> binary;
header_type(text_content_length) -> integer;
header_type(prop_content_length) -> integer;
header_type(revision_number) -> integer;
header_type(text_copy_source_md5) -> binary;
header_type(text_content_md5) -> binary;
header_type(uuid) -> binary;
header_type(text_delta) -> bool;
header_type(prop_delta) -> bool;
header_type(text_delta_base_md5) -> binary;
header_type(text_delta_base_sha1) -> binary;
header_type(text_copy_source_sha1) -> binary;
header_type(text_content_sha1) -> binary.

%% @doc Yields the name for a header.
header_name(content_length) -> <<"Content-length">>;
header_name(node_path) -> <<"Node-path">>;
header_name(node_kind) -> <<"Node-kind">>;
header_name(node_action) -> <<"Node-action">>;
header_name(node_copyfrom_rev) -> <<"Node-copyfrom-rev">>;
header_name(node_copyfrom_path) -> <<"Node-copyfrom-path">>;
header_name(text_content_length) -> <<"Text-content-length">>;
header_name(prop_content_length) -> <<"Prop-content-length">>;
header_name(revision_number) -> <<"Revision-number">>;
header_name(text_delta) -> <<"Text-delta">>;
header_name(prop_delta) -> <<"Prop-delta">>;
header_name(text_delta_base_md5) -> <<"Text-delta-base-md5">>;
header_name(text_delta_base_sha1) -> <<"Text-delta-base-sha1">>;
header_name(text_copy_source_md5) -> <<"Text-copy-source-md5">>;
header_name(text_copy_source_sha1) -> <<"Text-copy-source-sha1">>;
header_name(text_content_md5) -> <<"Text-content-md5">>;
header_name(text_content_sha1) -> <<"Text-content-sha1">>;
header_name(uuid) -> <<"UUID">>;
header_name(svn_fs_dump_format_version) -> <<"SVN-fs-dump-format-version">>.

scan_properties(Bin) ->
    scan_properties(Bin, []).

scan_properties(<<"PROPS-END\n">>, Ps) ->
    lists:reverse(Ps);
scan_properties(Bin, Ps) ->
    {P, Rest} = scan_property(Bin),
    case Rest of
	<<>> ->
	    throw(expected_PROPS_END);
	_ ->
	    scan_properties(Rest, [P|Ps])
    end.

scan_property(Bin) ->
    case scan_line(Bin) of
        {<<"K ", K/bytes>>, Rest0} ->
	    {Key, Rest1} = scan_nldata(scan_integer(K), Rest0),
	    case scan_line(Rest1) of
		{<<"V ", V/bytes>>, Rest2} ->
		    {Val, Rest3} = scan_nldata(scan_integer(V), Rest2),
		    {{Key, Val}, Rest3};
		X ->
		    throw({expected_prop_value, X})
	    end;
        {<<"D ", K/bytes>>, Rest0} ->
	    {Key, Rest1} = scan_nldata(scan_integer(K), Rest0),
            {{Key, delete}, Rest1};
        _ ->
	    throw(expected_prop_key)
    end.

%% @doc Extracts all svndump records from a binary.
scan_records(Bin) ->
    scan_records(Bin, []).

scan_records(Bin, Rs) ->
    case scan_record(Bin) of
	none ->
	    %% ignore trailing empty lines
	    lists:reverse(Rs);
	{R, Rest} ->
	    case Rest of
		<<>> ->
		    lists:reverse([R|Rs]);
		_ ->
		    scan_records(Rest, [R|Rs])
	    end
    end.

%% temporary generic record representation
-record(rec, {type,             %% version, uuid, revision, change
              info,             %% type-dependent data (kind for changes)
              properties,       %% list of properties (if any)
              content,          %% binary content data (if any)
              path,             %% node path (for changes)
              action,           %% node action (for changes)
              copypath,         %% copied from path (for adds-with-history)
              copyrev           %% copied from rev  (for adds-with-history)
             }).

scan_record(Bin) ->
    scan_record(Bin, []).

%% Returns {Record, Rest} | none
scan_record(Bin, Hs) ->
    case scan_line(Bin) of
        {<<>>, Rest} when Hs =:= [], Rest =/= <<>> ->
	    scan_record(Rest, Hs);  %% skip leading empty lines
        {<<>>, <<>>} when Hs =:= [] ->
            none;  %% no record found, only trailing empty lines
        {<<>>, Rest} ->
            make_record(Hs, Rest);
        {Line, Rest} ->
            scan_record(Rest, [scan_header(Line) | Hs])
    end.

make_record(Hs, Rest) ->
    {R, Hs1, Rest1} = make_record(Hs, [], #rec{}, Rest),
    %% sanity check intermediate record and build final representation
    R1 = case R of
             #rec{type = uuid, info = Id,
                  properties = undefined, content = undefined, 
                  path = undefined, action = undefined}
             when Id =/= undefined ->
                 #uuid{id = Id, headers = Hs1};
             #rec{type = version, info = N,
                  properties = undefined, content = undefined, 
                  path = undefined, action = undefined}
             when N =/= undefined ->
                 #version{number = N, headers = Hs1};
             #rec{type = revision, info = N,
                  properties = Ps, path = undefined, action = undefined} 
             when is_integer(N), N >= 0 ->
                 #revision{number=N, properties = Ps, headers = Hs1};
             #rec{type = change, info = Kind,
                  properties = Ps, content = Data,
                  path = Path, action = Action,
                  copypath = undefined, copyrev = undefined}
             when Path =/= undefined, Action =/= undefined ->
                 #change{path = Path, kind = Kind, action = Action,
                         properties = Ps, headers = Hs1, data = Data};
             #rec{type = change, info = Kind,
                  properties = Ps, content = Data,
                  path = Path, action = add,
                  copypath = FromPath, copyrev = FromRev}
             when Path =/= undefined,
                  FromPath =/= undefined, FromRev =/= undefined->
                 #change{path = Path, kind = Kind,
                         action = {add, FromPath, FromRev},
                         properties = Ps, headers = Hs1, data = Data};
             _ ->
                 throw({unknown_record, {R, Hs1, Rest1}})
         end,
    {R1, Rest1}.

%% Note: The incoming header list is here in reverse-occurrence order
make_record([{uuid, Id} | Hs], Hs1,
            #rec{type = undefined}=R, Rest) ->
    make_record(Hs, Hs1, R#rec{type = uuid, info = Id}, Rest);
make_record([{svn_fs_dump_format_version, N} | Hs], Hs1,
            #rec{type = undefined}=R, Rest) ->
    make_record(Hs, Hs1, R#rec{type = version, info = N}, Rest);
make_record([{content_length, Length} | Hs], Hs1,
            #rec{content = undefined}=R, Rest) ->
    %% If the record has any content, there must be a Content-length header
    {Data, Rest1} = scan_nldata(Length, Rest),
    ?assertEqual(byte_size(Data), Length),
    make_record(Hs, Hs1, R#rec{content=Data}, Rest1);
make_record([{prop_content_length, PLen} | Hs], Hs1,
            #rec{content = Data}=R, Rest)
  when Data =/= undefined ->
    %% If there are properties, there must be a Prop-content-length and
    %% we must have seen a Content-length so we have extracted the content
    {PData, TData} = split_binary(Data, PLen),
    ?assertEqual(byte_size(PData), PLen),
    ?assertEqual(PLen + byte_size(TData), byte_size(Data)),
    Ps = scan_properties(PData),
    make_record(Hs, Hs1, R#rec{content = TData, properties = Ps}, Rest);
make_record([{text_content_length, _} | Hs], Hs1,
            #rec{content = Data}=R, Rest)
  when Data =/= undefined ->
    %% Discard any text content length headers - will be recomputed on output
    make_record(Hs, Hs1, R, Rest);
make_record([{node_path, Path} | Hs], Hs1,
            #rec{type = T, path = undefined}=R, Rest)
  when T =:= undefined ; T =:= change ->
    make_record(Hs, Hs1, R#rec{type = change, path = Path}, Rest);
make_record([{node_kind, Kind} | Hs], Hs1,
            #rec{type = T, info = undefined}=R, Rest)
  when T =:= undefined ; T =:= change ->
    make_record(Hs, Hs1, R#rec{type = change, info = Kind}, Rest);
make_record([{node_action, Action} | Hs], Hs1,
            #rec{type = T, action = undefined}=R, Rest)
  when T =:= undefined ; T =:= change ->
    make_record(Hs, Hs1, R#rec{type = change, action = Action}, Rest);
make_record([{node_copyfrom_path, FromPath} | Hs], Hs1,
            #rec{type = T, copypath = undefined}=R, Rest)
  when T =:= undefined ; T =:= change ->
    make_record(Hs, Hs1, R#rec{type = change, copypath = FromPath}, Rest);
make_record([{node_copyfrom_rev, FromRev} | Hs], Hs1,
            #rec{type = T, copyrev = undefined}=R, Rest)
  when T =:= undefined ; T =:= change ->
    make_record(Hs, Hs1, R#rec{type = change, copyrev = FromRev}, Rest);
make_record([{revision_number, N} | Hs], Hs1,
            #rec{type = undefined, info=undefined}=R, Rest) ->
    put(revision_number, N),
    make_record(Hs, Hs1, R#rec{type = revision, info = N}, Rest);
make_record([H | Hs], Hs1, R, Rest) ->
    %% other headers are just collected in order of occurrence
    make_record(Hs, [H | Hs1], R, Rest);
make_record([], Hs, R, Rest) ->
    {R, Hs, Rest}.

%% @doc Applies a filter function (really map/fold/filter) to all records of
%% an SVN dump file. The new file gets the name of the input file with the
%% suffix ".filtered". The function gets a record and the current state, and
%% should return either `{true, NewState}' or`{true, NewRecord, NewState}'
%% if the (possibly modified) record should be kept, or `{false, NewState}'
%% if the record should be omitted from the output. The function returns the
%% final state.
filter(Infile, Fun, State0) ->
    Outfile = Infile ++ ".filtered",
    Out = open_outfile(Outfile),
    Fun1 = fun (R, St) ->
                   case Fun(R, St) of
                       {true, R1, St1} ->
                           file:write(Out, format_record(R1)), St1;
                       {true, St1} ->
                           file:write(Out, format_record(R)), St1;
                       {false, St1} ->
                           St1
                   end
           end,
    fold(Infile, Fun1, State0).

%% @doc Applies a fold function to all records of an SVN dump file. The
%% function gets a record and the current state, and should return the new
%% state. The function returns the final state.
fold(Infile, Fun, State0) ->
    with_infile(Infile,
                fun () ->
                        fold_1(<<>>, Fun, State0)
                end).

fold_1(Bin, Fun, State) ->
    case scan_record(Bin) of
        none ->
	    State;  % ignore trailing empty lines
	{R, Rest} ->
            NewState = Fun(R, State),
            case Rest of
		<<>> ->
		    NewState;
		_ ->
		    fold_1(Rest, Fun, NewState)
	    end
    end.

%% @doc Rewrites an SVN dump file to Erlang term format. The new file gets
%% the name of the input file with the suffix ".terms", and can be read
%% back using the Erlang standard library function file:consult().
to_terms(Infile) ->
    Outfile = Infile ++ ".terms",
    Out = open_outfile(Outfile),
    with_infile(Infile,
                fun () ->
                        to_terms(<<>>, Out)
                end),
    ok.

to_terms(Bin, Out) ->
    case scan_record(Bin) of
        none ->
	    ok;  % ignore trailing empty lines
	{R, Rest} ->
	    io:format(Out, "~p.\n", [R]),
	    case Rest of
		<<>> ->
		    ok;
		_ ->
		    to_terms(Rest, Out)
	    end
    end.

%% @doc Formats a list of records for output to an svndump file.
format_records(Rs) ->
    [format_record(R) || R <- Rs].

format_record(#version{number = N}) ->
    [format_header(svn_fs_dump_format_version, N), $\n];
format_record(#uuid{id = Id}) ->
    [format_header(uuid, Id), $\n];
format_record(#revision{number = N, properties = Ps}) ->
    format_record([{revision_number, N}], prop_chunk(Ps), <<>>);
format_record(#change{action = delete, path = Path,
		      headers = Hs, properties = Ps, data = Data}) ->
    Hs1 = [{node_path, flat_path(Path)},
	   {node_action, delete}
	   | Hs],
    format_record(Hs1, prop_chunk(Ps), Data);
format_record(#change{action = {add, FromPath, FromRev}, path = Path,
		      headers = Hs, properties = Ps,
                      data = Data}) ->
    Hs1 = [{node_path, flat_path(Path)},
	   {node_action, add},
           {node_copyfrom_path, flat_path(FromPath)},
           {node_copyfrom_rev, FromRev}
	   | Hs],
    format_record(Hs1, prop_chunk(Ps), Data);
format_record(#change{action = Action, kind = Kind, path = Path,
		      headers = Hs, properties = Ps, data = Data})
  when Kind =/= undefined, Action =/= undefined ->
    Hs1 = [{node_path, flat_path(Path)},
	   {node_kind, Kind},
	   {node_action, Action}
	   | Hs],
    format_record(Hs1, prop_chunk(Ps), Data).

flat_path(Path) when is_binary(Path) -> Path;
flat_path([Path]) when is_binary(Path) -> Path;
flat_path([]) -> <<>>;
flat_path([P | Ps]) when is_binary(P) ->
    list_to_binary([P | flat_path_1(Ps)]).

flat_path_1([P | Ps]) -> ["/", P | flat_path_1(Ps)];
flat_path_1([]) -> [].

format_record(Hs, undefined, undefined) ->
    [[format_header(H) || H <- Hs],
     [format_header(content_length, 0),  %% we always include this header
      $\n]];
format_record(Hs, undefined, Text) ->
    TLen = byte_size(Text),
    [[format_header(H) || H <- Hs],
     [format_header(text_content_length, TLen) || TLen > 0],
     [format_header(content_length, TLen),
      $\n, Text, $\n]];
format_record(Hs, Props, undefined) ->
    PLen = byte_size(Props), 
    [[format_header(H) || H <- Hs],
     [format_header(prop_content_length, PLen) || PLen > 0],
     [format_header(content_length, PLen),
      $\n, Props, $\n]];
format_record(Hs, Props, Text) ->
    PLen = byte_size(Props), 
    TLen = byte_size(Text),
    [[format_header(H) || H <- Hs],
     [format_header(prop_content_length, PLen) || PLen > 0],
     [format_header(text_content_length, TLen) || TLen > 0],
     [format_header(content_length, PLen + TLen),
      $\n, Props, Text, $\n]].

prop_chunk(undefined) ->
    undefined;
prop_chunk(Ps) ->
    iolist_to_binary([format_props(Ps),<<"PROPS-END\n">>]).

format_header({Name, Value}) ->
    format_header(Name, Value).

format_header(Name, Value) ->
    V = if is_integer(Value) ->
		integer_to_list(Value);
	   is_atom(Value) ->
		atom_to_list(Value);
	   is_binary(Value) ->
		Value
	end,
    [header_name(Name), <<": ">>, V, $\n].

format_props(Ps) ->
    [format_prop(P) || P <- Ps].

format_prop({Name, delete}) ->
    [<<"D ">>, integer_to_list(byte_size(Name)), $\n, Name, $\n];
format_prop({Name, Value}) ->
    [<<"K ">>, integer_to_list(byte_size(Name)), $\n, Name, $\n,
     <<"V ">>, integer_to_list(byte_size(Value)), $\n, Value, $\n].

scan_line(Bin) ->
    scan_line(0, Bin).

scan_line(N, Bin) ->
    case Bin of
        <<Line:N/bytes, "\n">> ->
            with_more(<<>>,
                      fun (eof) ->
                              {Line, <<>>};
                          (More) ->
                              {Line, More}
                      end);
        <<Line:N/bytes, "\n", Rest/bytes>> ->
            {Line, Rest};
        <<_:N/bytes, _/bytes>> ->
            scan_line(N+1, Bin);
        Rest ->
            with_more(Rest,
                      fun (eof) ->
                              {Rest, <<>>};
                          (More) ->
                              scan_line(0, More)
                      end)
    end.

lines(Bin) ->
    lines(Bin, []).

lines(Bin, Ls) ->
    case scan_line(Bin) of
        {L, <<>>} ->
            lists:reverse([L | Ls]);
        {L, Rest} ->
            lines(Rest, [L|Ls])
    end.

%% N bytes of data plus newline (not included in N)
scan_nldata(N, Bin) ->
    case Bin of
	<<Data:N/bytes, "\n">> ->
            with_more(<<>>,
                      fun (eof) ->
                              {Data, <<>>};
                          (More) ->
                              {Data, More}
                      end);
	<<Data:N/bytes, "\n", Rest/bytes>> ->
	    {Data, Rest};
        Rest ->
            with_more(Rest,
                      fun (eof) ->
                              throw(unexpected_end);
                          (More) ->
                              scan_nldata(N, More)
                      end)
    end.

%% ---- Internal unit tests ----

scan_line_test() ->
    ?assertEqual({<<"abcdef">>,<<>>}, scan_line(<<"abcdef">>)),
    ?assertEqual({<<"abcdef">>,<<>>}, scan_line(<<"abcdef\n">>)),
    ?assertEqual({<<"abcdef">>,<<"xyz">>}, scan_line(<<"abcdef\nxyz">>)),
    ?assertEqual({<<>>,<<"abcdef\nxyz">>}, scan_line(<<"\nabcdef\nxyz">>)).

lines_test() ->
    ?assertMatch([<<"">>,<<"a">>,<<"bc">>,<<"">>,<<"def">>,<<"g">>],
		 lines(<<"\na\nbc\n\ndef\ng">>)).

lines_file_test() ->
    {ok, Bin} = file:read_file("priv/example.dump"),
    lines(Bin).

scan_record_test() ->
    Data0 = <<"SVN-fs-dump-format-version: 2\n\nUUID: ABC123\n\n"
             "Revision-number: 123\nProp-content-length: 10\n"
	     "Content-length: 10\n\nPROPS-END\n\n">>,
    {#version{number = 2}, Data1} = scan_record(Data0),
    {#uuid{id = <<"ABC123">>}, Data2} = scan_record(Data1),
    {#revision{number = 123}, <<>>} = scan_record(Data2).

scan_empty_record_test_() ->
    [?_assertEqual(none,scan_record(<<>>)),
     ?_assertEqual(none,scan_record(<<"\n">>)),
     ?_assertEqual(none,scan_record(<<"\n\n">>))].

scan_properties_test() ->
    Data1 = <<"K 6\nauthor\nV 7\nsussman\nK 3\nlog\nV 33\n"
	    "Added two files, changed a third.\nPROPS-END\n">>,
    [{<<"author">>,<<"sussman">>},
     {<<"log">>,<<"Added two files, changed a third.">>}
    ] = scan_properties(Data1),
    Data2 = <<"D 6\nauthor\nPROPS-END\n">>,
    [{<<"author">>,delete}] = scan_properties(Data2).
