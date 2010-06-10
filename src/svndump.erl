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
	    %% If the record has content, the last header is Content-length
	    case Hs of
		[{content_length, Length}|Hs1] ->
		    {Data, Rest1} = scan_nldata(Length, Rest),
		    %% If there are properties, there should be a
		    %% Prop-content-length header somewhere.
		    case get_prop_content(Hs1) of
			none ->
			    {{lists:reverse(Hs), none, Data}, Rest1};
			PLen ->
			    {PData, TData} = scan_data(PLen, Data),
			    ?assertEqual(byte_size(PData), PLen),
			    ?assertEqual(PLen + byte_size(TData), Length),
			    {{lists:reverse(Hs),
			      scan_properties(PData),
			      TData}, Rest1}
		    end;
		_ ->
		    {{lists:reverse(Hs), none, <<>>}, Rest}
	    end;
        {Line, Rest} ->
            scan_record(Rest, [scan_header(Line) | Hs])
    end.

get_prop_content([{prop_content_length, PLen}|_]) -> PLen;
get_prop_content([_|Hs]) -> get_prop_content(Hs);
get_prop_content([]) -> none.

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
	    file:write(Out, termify_record(R)),
	    case Rest of
		<<>> ->
		    ok;
		_ ->
		    dump_to_terms(Rest, Out)
	    end
    end.

termify_record({Hs, Ps, B}) ->
    [<<"{[">>,
     termify_headers(Hs),
     <<"],\n [">>,
     termify_props(Ps),
     <<"],\n ">>,
     termify_bin(B),
     <<"}.\n">>].

termify_headers([]) ->
    <<"<<>>">>;
termify_headers([H|Hs]) ->
    [termify_header(H), [[<<",\n  ">>, termify_header(H1)] || H1 <- Hs]].

termify_header({Name, Value}) ->
    V = if is_integer(Value) ->
		integer_to_list(Value);
	   is_atom(Value) ->
		atom_to_list(Value);
	   is_binary(Value) ->
		termify_bin(Value)
	end,
    [<<"{">>, atom_to_list(Name), <<", ">>, V, <<"}">>].

termify_props([]) ->
    <<"<<>>">>;
termify_props([P|Ps]) ->
    [termify_prop(P), [[<<",\n  ">>, termify_prop(P1)] || P1 <- Ps]].

termify_prop({Name, Value}) ->
    [<<"{">>, termify_bin(Name), <<", ">>, termify_bin(Value), <<"}">>].

termify_bin(B) ->
     io_lib:format("~p",[B]).

update_headers([{content_length, _} | Hs], PLen, TLen) ->
    [{content_length, PLen + TLen} | update_headers(Hs, PLen, TLen)];
update_headers([{prop_content_length, _} | Hs], PLen, TLen) ->
    [{prop_content_length, PLen} | update_headers(Hs, PLen, TLen)];
update_headers([{text_content_length, _} | Hs], PLen, TLen) ->
    [{text_content_length, TLen} | update_headers(Hs, PLen, TLen)];
update_headers([H | Hs], PLen, TLen) ->
    [H | update_headers(Hs, PLen, TLen)];
update_headers([], _, _) ->
    [].

%% @doc Formats a list of records for output to an svndump file.
format_records(Rs) ->
    [format_record(R) || R <- Rs].

format_record({Hs, none, Text}) ->
    Hs1 = update_headers(Hs, 0, byte_size(Text)),
    [format_headers(Hs1), $\n, Text, $\n];
format_record({Hs, Ps, Text}) ->
    Props = iolist_to_binary([format_props(Ps),<<"PROPS-END\n">>]),
    Hs1 = update_headers(Hs, byte_size(Props), byte_size(Text)),
    [format_headers(Hs1), $\n, Props, Text, $\n].

format_headers(Hs) ->
    [format_header(H) || H <- Hs].

format_header({Name, Value}) ->
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
             "Revision-number: 123\nContent-length: 10\n\n0123456789\n">>,
    {{[{svn_fs_dump_format_version,2}], none, <<>>},
     Data1} = scan_record(Data0),
    {{[{uuid,<<"ABC123">>}], none, <<>>},
     Data2} = scan_record(Data1),
    {{[{revision_number,123},
       {content_length,10}],
      none, <<"0123456789">>},
     <<>>} = scan_record(Data2).

scan_properties_test() ->
    Data = <<"K 6\nauthor\nV 7\nsussman\nK 3\nlog\nV 33\n"
	    "Added two files, changed a third.\nPROPS-END\n">>,
    [{<<"author">>,<<"sussman">>},
     {<<"log">>,<<"Added two files, changed a third.">>}
    ] = scan_properties(Data).
