%% ---------------------------------------------------------------------
%% File: svndump.erl
%%
%% @author Richard Carlsson <richardc@klarna.com>
%% @copyright 2010 Richard Carlsson
%% @doc Functions for working with SVN dumpfiles.

-module(svndump).

-export([dump_to_terms/1, scan_records/1, header_vsn/1, header_default/1,
	 header_type/1, header_name/1, format_records/1, filter_dump/2]).

-include_lib("eunit/include/eunit.hrl").

-include("svndump.hrl").

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
        _ ->
	    throw(expected_prop_key)
    end.

%% N bytes of data plus newline (not included in N)
scan_nldata(N, Bin) ->
    case Bin of
	<<Data:N/bytes, "\n", Rest/bytes>> ->
	    {Data, Rest};
	_ ->
	    throw(unexpected_end)
    end.

scan_data(N, Bin) ->
    case Bin of
	<<Data:N/bytes, Rest/bytes>> ->
	    {Data, Rest};
	_ ->
	    throw(unexpected_end)
    end.

%% @doc Extracts all svndump records from a binary.
scan_records(Bin) ->
    scan_records(Bin, []).

scan_records(Bin, Rs) ->
    case scan_record(Bin) of
	{{[], [], <<>>}, <<>>} ->
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

scan_record(Bin) ->
    scan_record(Bin, []).

%% Returns {Headers, Properties, Body}. Properties is 'none' if there
%% was no Prop-content-length header, otherwise it's a list.
scan_record(Bin, Hs) ->
    case scan_line(Bin) of
        {<<>>, Rest} when Hs =:= [], Rest =/= <<>> ->
	    scan_record(Rest, Hs);  %% skip leading empty lines
        {<<>>, Rest} ->
	    make_record(Hs, [], none, [], none, Rest);
        {Line, Rest} ->
            scan_record(Rest, [scan_header(Line) | Hs])
    end.

%% The header list is here in reverse-occurrence order
make_record([{uuid, Id}], [], _R, [], none, Rest) ->
    {#uuid{id = Id}, Rest};
make_record([{svn_fs_dump_format_version, N}], [], _R, [], none, Rest) ->
    {#version{number = N}, Rest};
make_record([{content_length, Length} | Hs], [], R, [], none, Rest) ->
    %% If the record has any content, the last header is Content-length
    {Data, Rest1} = scan_nldata(Length, Rest),
    ?assertEqual(byte_size(Data), Length),
    make_record(Hs, [], R, [], Data, Rest1);
make_record([{prop_content_length, PLen} | Hs], Hs1, R, [], Data, Rest)
  when is_binary(Data) ->
    %% If there are properties, there should be a Prop-content-length and
    %% we must have seen a Content-length so we have extracted the content
    {PData, TData} = scan_data(PLen, Data),
    ?assertEqual(byte_size(PData), PLen),
    ?assertEqual(PLen + byte_size(TData), byte_size(Data)),
    Ps = scan_properties(PData),
    make_record(Hs, Hs1, R, Ps, TData, Rest);
make_record([{text_content_length, _} | Hs], Hs1, R, Ps, Data, Rest)
  when is_binary(Data) ->
    %% Discard this header - will be computed on output
    make_record(Hs, Hs1, R, Ps, Data, Rest);
make_record([{node_path, Path} | Hs], Hs1, #change{path=undefined}=R, Ps,
	    Data, Rest) ->
    make_record(Hs, Hs1, R#change{path=Path}, Ps, Data, Rest);
make_record([{node_path, Path} | Hs], Hs1, none, Ps, Data, Rest) ->
    make_record(Hs, Hs1, #change{path=Path}, Ps, Data, Rest);
make_record([{node_kind, Kind} | Hs], Hs1, #change{kind=undefined}=R, Ps,
	    Data, Rest) ->
    make_record(Hs, Hs1, R#change{kind=Kind}, Ps, Data, Rest);
make_record([{node_kind, Kind} | Hs], Hs1, none, Ps, Data, Rest) ->
    make_record(Hs, Hs1, #change{kind=Kind}, Ps, Data, Rest);
make_record([{node_action, Action} | Hs], Hs1, #change{action=undefined}=R,
	    Ps, Data, Rest) ->
    make_record(Hs, Hs1, R#change{action=Action}, Ps, Data, Rest);
make_record([{node_action, Action} | Hs], Hs1, none, Ps, Data, Rest) ->
    make_record(Hs, Hs1, #change{action=Action}, Ps, Data, Rest);
make_record([{revision_number, N} | Hs], Hs1, #revision{number=undefined}=R,
	    Ps, Data, Rest) ->
    make_record(Hs, Hs1, R#revision{number=N}, Ps, Data, Rest);
make_record([{revision_number, N} | Hs], Hs1, none, Ps, Data, Rest) ->
    make_record(Hs, Hs1, #revision{number=N}, Ps, Data, Rest);
make_record([H | Hs], Hs1, R, Ps, Data, Rest) ->
    make_record(Hs, [H | Hs1], R, Ps, Data, Rest);
make_record([], [], R=#revision{}, Ps, <<>>, Rest) ->
    {R#revision{properties=Ps}, Rest};
make_record([], Hs1, R=#change{}, Ps, Data, Rest) ->
    {R#change{headers=Hs1, properties=Ps, data=Data}, Rest};
make_record([], Hs1, R, Ps, Data, _Rest) ->
    throw({unknown_record, {Hs1, R, Ps, Data}}).

open_write(File) ->
    {ok, FD} = file:open(File, [write]),
    FD.

%% @doc Applies a filter function to all records of an SVN dump file. The
%% new file gets the name of the input file with the suffix ".filtered".
filter_dump(Infile, Fun) ->
    Outfile = Infile ++ ".filtered",
    Out = open_write(Outfile),
    {ok, Bin} = file:read_file(Infile),
    filter_dump(Bin, Out, Fun),
    file:close(Out),
    ok.

filter_dump(Bin, Out, Fun) ->
    case scan_record(Bin) of
	{{[], [], <<>>}, <<>>} ->
	    ok;  % ignore trailing empty lines
	{R, Rest} ->
	    file:write(Out, format_record(Fun(R))),
	    case Rest of
		<<>> ->
		    ok;
		_ ->
		    filter_dump(Rest, Out, Fun)
	    end
    end.

%% @doc Rewrites an SVN dump file to Erlang term format. The new file gets
%% the name of the input file with the suffix ".terms", and can be read
%% back using the Erlang standard library function file:consult().
dump_to_terms(Infile) ->
    Outfile = Infile ++ ".terms",
    Out = open_write(Outfile),
    {ok, Bin} = file:read_file(Infile),
    dump_to_terms(Bin, Out),
    file:close(Out),
    ok.

dump_to_terms(Bin, Out) ->
    case scan_record(Bin) of
	{{[], [], <<>>}, <<>>} ->
	    ok;  % ignore trailing empty lines
	{R, Rest} ->
	    io:format(Out, "~p.\n", [R]),
	    case Rest of
		<<>> ->
		    ok;
		_ ->
		    dump_to_terms(Rest, Out)
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
format_record(#change{path = Path, kind = Kind, action = Action,
		      headers = Hs, properties = Ps, data = Data}) ->
    Hs1 = [{node_path, Path},
	   {node_kind, Kind},
	   {node_action, Action}
	   | Hs],
    format_record(Hs1, prop_chunk(Ps), Data).

format_record(Hs, Props, Text) ->
    PLen = byte_size(Props), 
    TLen = byte_size(Text),
    [[format_header(H) || H <- Hs],
     [format_header(prop_content_length, PLen) || PLen > 0],
     [format_header(text_content_length, TLen) || TLen > 0],
     [format_header(content_length, PLen + TLen),
      $\n, Props, Text, $\n]].

prop_chunk([]) ->
    <<>>;
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

format_prop({Name, Value}) ->
    [<<"K ">>, integer_to_list(byte_size(Name)), $\n, Name, $\n,
     <<"V ">>, integer_to_list(byte_size(Value)), $\n, Value, $\n].

scan_line(Bin) ->
    scan_line(0, Bin).
                       
scan_line(N, Bin) ->
    case Bin of
        <<Line:N/bytes, "\n", Rest/bytes>> ->
            {Line, Rest};
        <<_:N/bytes, _/bytes>> ->
            scan_line(N+1, Bin);
        Rest ->
            {Rest, <<>>}
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

%% ---- Internal unit tests ----

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

scan_properties_test() ->
    Data = <<"K 6\nauthor\nV 7\nsussman\nK 3\nlog\nV 33\n"
	    "Added two files, changed a third.\nPROPS-END\n">>,
    [{<<"author">>,<<"sussman">>},
     {<<"log">>,<<"Added two files, changed a third.">>}
    ] = scan_properties(Data).
