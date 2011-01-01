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
%% @author Richard Carlsson <carlsson.richardc@gmail.com>
%% @copyright 2010 Richard Carlsson
%% @doc Library for working with SVN dumpfiles.

%% Notes: For simplicity, paths may be represented as single binaries (as
%% returned by the dumpfile scanner), or as lists of binaries: for example,
%% [<<"foo">>, <<"bar">>, <<"baz">>] representing <<"foo/bar/baz">>. Also,
%% the svn:mergeinfo property value is preprocessed by the parser, and is
%% given as a list of {Path, Ranges} tuples, where Ranges is a list of
%% single revision numbers and/or {From, To} revision number pairs. For
%% example, <<"foo/trunk:1-3,5,7-11">> may be represented as
%% [{<<"foo/trunk">>, [{1,3},5,{7-11}]}] or [{[<<"foo">>,<<"trunk">>],
%% [{1,3},{5,5},{7-11}]}], or some other variant.

-module(svndump).

-behaviour(gen_server).

%% API
-export([filter/3, fold/3, to_terms/1]).
-export([scan_records/1, header_vsn/1, header_default/1, header_type/1,
	 header_name/1, format_records/1, scan_mergeinfo/1,
         normalize_ranges/1, join_path/1, cache_bin/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {file}).


-include_lib("eunit/include/eunit.hrl").

-include("../include/svndump.hrl").

-define(CHUNK_SIZE, (8*1024*1024)).

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
%% @private
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
%% @private
handle_call({apply, Fun}, _From, State) ->
    try
        Reply = Fun(),
        {reply, Reply, State}
    catch C:T ->
            %% we need to clear the process dict from tracked paths;
            %% otherwise, the OTP error reporting will crash the system when
            %% it tries to handle all the data in the dictionary
            [put(K, V) || {K,V} <- erase(), not is_integer(K)],
            file:close(State#state.file),
            error_logger:format("last revision: ~p\n"
                                "exception: ~p:~p\n"
                                "stack trace: ~p\n",
                                [get(latest_rev), C, T,
                                 erlang:get_stacktrace()]),
            {stop, error, State}
    end.

%% ---------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%% ---------------------------------------------------------------------
%% @private
handle_cast(stop, State) ->
    {stop, normal, State}.

%% ---------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%% ---------------------------------------------------------------------
%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% ---------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% ---------------------------------------------------------------------
%% @private
terminate(_Reason, _State) ->
    ok.

%% ---------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%% ---------------------------------------------------------------------
%% @private
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
    %% force GC here to keep memory usage lower (well worth the extra CPU
    %% usage - if you start swapping, it costs a lot more in total time)
    garbage_collect(),
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
%% * Content-length is always the last header before the blank line and
%% content.
%% * Content-length should always be included, but is sometimes omitted,
%%   notably in delete-nodes written by the standard svndump.
%% * For directory nodes, the content is only property data, no text data.
%% * Text-content-length should always be included if there is any content
%% apart from properties. Otherwise, you can't tell the difference between a
%% zero-length text content and no text content.

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
    {text_delta, scan_boolean(B)}; % contents treated as delta
scan_header(<<"Prop-delta: ", B/bytes>>) ->
    {prop_delta, scan_boolean(B)}; % contents treated as delta
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

scan_boolean(<<"true">>) -> true;
scan_boolean(<<"false">>) -> false.

scan_node_kind(<<"file">>) -> file;
scan_node_kind(<<"dir">>) -> dir.

scan_node_action(<<"change">>) -> change;
scan_node_action(<<"add">>) -> add;
scan_node_action(<<"delete">>) -> delete;
scan_node_action(<<"replace">>) -> replace.

scan_integer(B) -> list_to_integer(binary_to_list(B)).

%% @spec header_vsn(Header::atom()) -> integer()
%% @doc Yields the minimum svndump version (1-3) for a header.
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

%% @spec header_default(Header::atom()) -> integer() | atom() | binary()
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

%% @spec header_type(Header::atom()) -> atom | binary | boolean | integer
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
header_type(text_delta) -> boolean;
header_type(prop_delta) -> boolean;
header_type(text_delta_base_md5) -> binary;
header_type(text_delta_base_sha1) -> binary;
header_type(text_copy_source_sha1) -> binary;
header_type(text_content_sha1) -> binary.

%% @spec header_name(Header::atom()) -> binary()
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
                    case Key of
                        <<"svn:mergeinfo">> ->
                            {{Key, scan_mergeinfo(Val)}, Rest3};
                        _ ->
                            {{Key, Val}, Rest3}
                    end;
		X ->
		    throw({expected_prop_value, X})
	    end;
        {<<"D ", K/bytes>>, Rest0} ->
	    {Key, Rest1} = scan_nldata(scan_integer(K), Rest0),
            {{Key, delete}, Rest1};
        _ ->
	    throw(expected_prop_key)
    end.

%% @type range() = integer() | {integer(),integer()}
%% @spec scan_mergeinfo(binary()) -> [{Path::binary(), [range()]}]
%% @doc Extracts symbolic merge information from an `svn:mergeinfo'
%% property body.
scan_mergeinfo(Bin) ->
    [{Path, [scan_range(R) || R <- Ranges]}
     || [Path | Ranges] <- [re:split(M, ":|,")
                            || M <- re:split(Bin, "\n")],
        Path =/= <<"">>, Ranges =/= []].

scan_range(Bin) ->
    case re:split(Bin, "-") of
        [R, R] ->
            scan_integer(R);
        [R1, R2] ->
            {scan_integer(R1), scan_integer(R2)};
        [R] ->
            scan_integer(R)
    end.

%% @spec scan_records(binary()) -> [term()]
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
              textlength,       %% undefined if no text content
              content,          %% text content data (if any), as binary
              path,             %% node path (for changes)
              action,           %% node action (for changes)
              md5,              %% MD5 sum of text content
              copypath,         %% copied from path (for adds-with-history)
              copyrev,          %% copied from rev  (for adds-with-history)
              copymd5           %% MD5 sum of copy source 
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

%% Make sure that the text content matches the text-content-length header.
%% If there was no such header, there should be no text content. We may get
%% an empty binary as remainder after splitting out properties, but that
%% doesn't automatically mean that there's text content present.
make_text(undefined, _) ->
    undefined;
make_text(TLen, Bin) when is_binary(Bin) ->
    ?assertEqual(byte_size(Bin), TLen), 
    Bin.

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
                  properties = Ps, content = Data, textlength = TLen,
                  path = Path, action = Action, md5=MD5,
                  copypath = undefined, copyrev = undefined}
             when Path =/= undefined, Action =/= undefined ->
                 #change{path = Path, kind = Kind, action = Action,
                         properties = Ps, headers = Hs1,
                         md5 = MD5, data = make_text(TLen, Data)};
             #rec{type = change, info = Kind,
                  properties = Ps, content = Data, textlength = TLen,
                  path = Path, action = add, md5=MD5,
                  copypath = FromPath, copyrev = FromRev, copymd5=FromMD5}
             when Path =/= undefined,
                  FromPath =/= undefined, FromRev =/= undefined->
                 #change{path = Path, kind = Kind,
                         action = {add, FromPath, FromRev, FromMD5},
                         properties = Ps, headers = Hs1,
                         md5=MD5, data = make_text(TLen, Data)};
             _ ->
                 throw({unknown_record, {R, Hs1, Rest1}})
         end,
    {R1, Rest1}.

%% Note: The incoming header list is here in reverse-occurrence order
make_record([{uuid, Id} | Hs], Hs1, #rec{type = T}=R, Rest) ->
    ?assert(T =:= undefined),
    make_record(Hs, Hs1, R#rec{type = uuid, info = Id}, Rest);
make_record([{svn_fs_dump_format_version, N} | Hs], Hs1,
            #rec{type = T}=R, Rest) ->
    ?assert(T =:= undefined),
    make_record(Hs, Hs1, R#rec{type = version, info = N}, Rest);
make_record([{content_length, Length} | Hs], Hs1,
            #rec{content = Data0}=R, Rest) ->
    %% If the record has any content, there must be a Content-length header
    ?assert(Data0 =:= undefined),
    {Data, Rest1} = scan_nldata(Length, Rest),
    ?assertEqual(byte_size(Data), Length),
    make_record(Hs, Hs1, R#rec{content=Data}, Rest1);
make_record([{prop_content_length, PLen} | Hs], Hs1,
            #rec{content = Data, properties = Ps0}=R, Rest) ->
    %% If there are properties, there must be a Prop-content-length and
    %% we must have seen a Content-length so we have extracted the content
    ?assert(Ps0 =:= undefined),
    ?assert(PLen > 0),
    ?assert(Data =/= undefined),
    {PData, TData} = split_binary(Data, PLen),
    ?assertEqual(byte_size(PData), PLen),
    ?assertEqual(PLen + byte_size(TData), byte_size(Data)),
    Ps = scan_properties(PData),
    make_record(Hs, Hs1, R#rec{content = TData, properties = Ps}, Rest);
make_record([{text_content_length, TLen} | Hs], Hs1,
            #rec{content = Data}=R, Rest) ->
    ?assert(Data =/= undefined),
    %% Note that text content length 0 is not the same as no text content!
    make_record(Hs, Hs1, R#rec{textlength = TLen}, Rest);
make_record([{node_path, Path} | Hs], Hs1,
            #rec{type = T, path = Path0}=R, Rest) ->
    ?assert(Path0 =:= undefined),
    ?assert(T =:= undefined orelse T =:= change),
    make_record(Hs, Hs1, R#rec{type = change, path = Path}, Rest);
make_record([{node_kind, Kind} | Hs], Hs1,
            #rec{type = T, info = Kind0}=R, Rest) ->
    ?assert(Kind0 =:= undefined),
    ?assert(T =:= undefined orelse T =:= change),
    make_record(Hs, Hs1, R#rec{type = change, info = Kind}, Rest);
make_record([{node_action, Action} | Hs], Hs1,
            #rec{type = T, action = Action0}=R, Rest) ->
    ?assert(Action0 =:= undefined),
    ?assert(T =:= undefined orelse T =:= change),
    make_record(Hs, Hs1, R#rec{type = change, action = Action}, Rest);
make_record([{node_copyfrom_path, FromPath} | Hs], Hs1,
            #rec{type = T, copypath = FromPath0}=R, Rest) ->
    ?assert(FromPath0 =:= undefined),
    ?assert(T =:= undefined orelse T =:= change),
    make_record(Hs, Hs1, R#rec{type = change, copypath = FromPath}, Rest);
make_record([{node_copyfrom_rev, FromRev} | Hs], Hs1,
            #rec{type = T, copyrev = FromRev0}=R, Rest) ->
    ?assert(FromRev0 =:= undefined),
    ?assert(T =:= undefined orelse T =:= change),
    make_record(Hs, Hs1, R#rec{type = change, copyrev = FromRev}, Rest);
make_record([{text_content_md5, MD5} | Hs], Hs1,
            #rec{type = T, md5 = MD50}=R, Rest) ->
    ?assert(MD50 =:= undefined),
    ?assert(T =:= undefined orelse T =:= change),
    make_record(Hs, Hs1, R#rec{type = change, md5 = MD5}, Rest);
make_record([{text_copy_source_md5, MD5} | Hs], Hs1,
            #rec{type = T, copymd5 = MD50}=R, Rest) ->
    ?assert(MD50 =:= undefined),
    ?assert(T =:= undefined orelse T =:= change),
    make_record(Hs, Hs1, R#rec{type = change, copymd5 = MD5}, Rest);
make_record([{revision_number, N} | Hs], Hs1,
            #rec{type = T, info=N0}=R, Rest) ->
    ?assert(T =:= undefined),
    ?assert(N0 =:= undefined),
    make_record(Hs, Hs1, R#rec{type = revision, info = N}, Rest);
make_record([H | Hs], Hs1, R, Rest) ->
    %% other headers are just collected in order of occurrence
    make_record(Hs, [H | Hs1], R, Rest);
make_record([], Hs, R, Rest) ->
    {R, Hs, Rest}.


%% @spec filter(Infile::string(), Fun::function(), State0::term()) -> term()
%% @doc Applies a filter function (really map/fold/filter) to all records of
%% an SVN dump file. The new file gets the name of the input file with the
%% suffix ".filtered".
%%
%% The filter function gets a record and the current state, and should
%% return either `{true, NewState}' or`{true, NewRecord, NewState}' if the
%% (possibly modified) record should be kept, or `{false, NewState}' if the
%% record should be omitted from the output. `NewRecord' can also be a list
%% of records, typically for splitting or duplicating a change, creating
%% missing paths, and so on. The result of the call to `filter/3' is the
%% final state. If the filter function evaluates `put(dry_run, true)' while
%% handling the first record (this is always the `#version{}' record), no
%% output will be written.

filter(Infile, Fun, State0) ->
    Outfile = Infile ++ ".filtered",
    Out = open_outfile(Outfile),
    Fun1 = fun (R, St) ->
                   case R of
                       #version{} ->
                           %% initialization is done here, at the first
                           %% record of the dump (recall that this code is
                           %% executed by a separate process)
                           put(dry_run, false),
                           put(update_md5, true),
                           put(latest_rev, 0),
                           %% map revision 0 to the empty tree
                           put(0, gb_trees:empty()),
                           ok;
                       #revision{number=N} ->
                           %% map the new revision to the same path tree as
                           %% the previous revision
                           put(N, get(get(latest_rev))),
                           %% update the latest revison number
                           put(latest_rev, N);
                       _ ->
                           ok
                   end,
                   case Fun(R, St) of
                       {true, R1, St1} ->
                           write_records(R1, Out), % possibly a list
                           St1;
                       {true, St1} ->
                           write_record(R, Out),
                           St1;
                       {false, St1} ->
                           St1
                   end
           end,
    fold(Infile, Fun1, State0).

write_records([R | Rs], Out) ->
    write_record(R, Out), write_records(Rs, Out);
write_records([], _Out) ->
    ok;
write_records(R, Out) ->
    write_record(R, Out).

write_record(R0, Out) ->
    R = case get(update_md5) of
            true -> update_md5(R0);
            false -> R0
        end,
    track_paths(R),
    Data = format_record(R),
    case get(dry_run) of
        true -> ok;
        false -> file:write(Out, Data)
    end.

split_path(P) when is_binary(P) ->
    re:split(P, "/");
split_path(P) ->
    P.

%% track and sanity check paths
track_paths(#change{action=Action, kind=Kind, path=Path, md5=MD5}) ->
    modify_path(split_path(Path), Kind, Action, get(latest_rev), MD5),
    ok;
track_paths(_R) ->
    ok.

%% recompute md5 sums to match actual content in case it's been modified
update_md5(#change{action=Action0, data=Data}=R) ->
    Action = case Action0 of
                 {add, FromPath, FromRev, _} ->
                     case find_node(split_path(FromPath), FromRev) of
                         Node when is_tuple(Node) ->
                             %% no MD5 on directories
                             {add, FromPath, FromRev, undefined};
                         FromMD5 ->
                             {add, FromPath, FromRev, FromMD5}
                     end;
                 _ ->
                     Action0
             end,
    MD5 = if is_binary(Data) -> md5_bin(Data);
             true -> undefined
          end,
    R#change{action=Action, md5 = MD5};
update_md5(R) ->
    R.

md5_bin(Data) ->
    << <<(hexchar(B div 16)), (hexchar(B rem 16))>>
     || <<B>> <= erlang:md5(Data) >>.

hexchar(N) when N >= 0, N < 10 -> $0 + N;    
hexchar(N) when N >= 10, N < 16 -> $a + N - 10.

%% @spec fold(Infile::string(), Fun::function(), State0::term()) -> term()
%% @doc Applies a fold function to all records of an SVN dump file. The
%% fold function gets a record and the current state, and should return the
%% new state. The result of the call to `fold/3' is the final state.

fold(Infile, Fun, State0) ->
    %% runs in separate server process
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

%% @spec to_terms(Infile::string()) -> ok
%% @doc Rewrites an SVN dump file to Erlang term format. The new file gets
%% the name of the input file with the suffix ".terms", and can be read
%% back using the Erlang standard library function file:consult().

to_terms(Infile) ->
    Outfile = Infile ++ ".terms",
    Out = open_outfile(Outfile),
    %% runs in separate server process
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


%% @spec format_records([term()]) -> iolist()
%% @doc Formats a list of records for output to an svndump file.
format_records(Rs) ->
    [format_record(R) || R <- Rs].

format_record(#version{number = N}) ->
    [format_header(svn_fs_dump_format_version, N), $\n];
format_record(#uuid{id = Id}) ->
    [format_header(uuid, Id), $\n];
format_record(#revision{number = N, properties = Ps}) ->
    format_record([{revision_number, N}], prop_chunk(Ps), undefined);
format_record(#change{action = delete, path = Path,
		      headers = Hs, properties = Ps, data = Data}) ->
    Hs1 = [{node_path, join_path(Path)},
	   {node_action, delete}
	   | Hs],
    format_record(Hs1, prop_chunk(Ps), Data);
format_record(#change{action = {add, FromPath, FromRev, FromMD5},
                      path = Path, headers = Hs, properties = Ps,
                      md5 = MD5, data = Data}) ->
    Hs1 = ([{node_path, join_path(Path)},
            {node_action, add},
            {node_copyfrom_rev, FromRev},
            {node_copyfrom_path, join_path(FromPath)}]
           ++ [{text_copy_source_md5, FromMD5} || FromMD5 =/= undefined]
           ++ [{text_content_md5, MD5} || MD5 =/= undefined]
           ++ Hs),
    format_record(Hs1, prop_chunk(Ps), Data);
format_record(#change{action = Action, kind = Kind, path = Path,
		      headers = Hs, properties = Ps,
                      md5 = MD5, data = Data})
  when Kind =/= undefined, Action =/= undefined ->
    Hs1 = ([{node_path, join_path(Path)},
            {node_kind, Kind},
            {node_action, Action}]
           ++ [{text_content_md5, MD5} || MD5 =/= undefined]
           ++ Hs),
    format_record(Hs1, prop_chunk(Ps), Data).

format_record(Hs, undefined, undefined) ->
    [[format_header(H) || H <- Hs],
     [format_header(content_length, 0),  %% always include this header
      $\n]];
format_record(Hs, undefined, Text) ->
    TLen = byte_size(Text),
    [[format_header(H) || H <- Hs],
     [format_header(text_content_length, TLen),
      format_header(content_length, TLen),
      $\n, Text, $\n]];
format_record(Hs, Props, undefined) ->
    PLen = byte_size(Props), 
    [[format_header(H) || H <- Hs],
     [format_header(prop_content_length, PLen),
      format_header(content_length, PLen),
      $\n, Props, $\n]];
format_record(Hs, Props, Text) ->
    PLen = byte_size(Props), 
    TLen = byte_size(Text),
    [[format_header(H) || H <- Hs],
     [format_header(prop_content_length, PLen),
      format_header(text_content_length, TLen),
      format_header(content_length, PLen + TLen),
      $\n, Props, Text, $\n]].

%% @spec join_path(Path) -> binary()
%%  where Path = binary() | [binary()]
%% @doc Joins two paths, as represented by this module. Exported so that it
%% may be called from filter functions.

join_path([]) -> <<>>;
join_path([Path]) when is_binary(Path) -> Path;
join_path([P | Ps]) -> list_to_binary([P | join_path_1(Ps)]);
join_path(Path) when is_binary(Path) -> Path.

join_path_1([P | Ps]) -> ["/", P | join_path_1(Ps)];
join_path_1([]) -> [].

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

format_prop({<<"svn:mergeinfo">> = P, Ms}) when is_list(Ms) ->
    format_prop({P, iolist_to_binary(format_mergeinfo(Ms))});
format_prop({Name, delete}) ->
    [<<"D ">>, integer_to_list(byte_size(Name)), $\n, Name, $\n];
format_prop({Name, Value}) ->
    [<<"K ">>, integer_to_list(byte_size(Name)), $\n, Name, $\n,
     <<"V ">>, integer_to_list(byte_size(Value)), $\n, Value, $\n].

format_mergeinfo([{Path, Ranges}]) ->
    [join_path(Path), $:, format_ranges(normalize_ranges(Ranges))];
format_mergeinfo([{Path, Ranges} | Ms]) ->
    [join_path(Path), $:, format_ranges(normalize_ranges(Ranges)), $\n
     | format_mergeinfo(Ms)];
format_mergeinfo([]) -> [].

format_ranges([R]) ->
    [format_range(R)];
format_ranges([R | Rs]) ->
    [format_range(R), $,, format_ranges(Rs)];
format_ranges([]) -> [].

format_range({R, R}) when is_integer(R), R >= 0 ->
    integer_to_list(R);
format_range({R1, R2})
  when is_integer(R1), is_integer(R2), R1 >= 0, R2 > R1  ->
    [integer_to_list(R1), $-, integer_to_list(R2)];
format_range(R) when is_integer(R), R >= 0 ->
    integer_to_list(R).

%% @spec normalize_ranges([range()]) -> [range()]
%% @doc Ensures that `svn:mergeinfo' ranges is in the expected normal form.
normalize_ranges(Rs) ->
    merge_ranges(lists:sort([normalize_range(R) || R <- Rs])).

normalize_range({R, R}=R0) when is_integer(R), R >= 0 ->
    R0;
normalize_range({R1, R2}=R0)
  when is_integer(R1), is_integer(R2), R1 >= 0, R2 > R1  ->
    R0;
normalize_range(R) when is_integer(R), R >= 0 ->
    {R, R}.

merge_ranges([{R1, R2}, {R3, R4} | Rs]) when (R3 - R2) =< 1 ->
    merge_ranges([{R1, R4} | Rs]);
merge_ranges([R | Rs]) ->
    [R | merge_ranges(Rs)];
merge_ranges([]) ->
    [].

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

%% We need some infrastructure to keep track of valid paths, but despite the
%% overhead, having this sanity check is well worth it, in particular on
%% large repositories, so you don't discover mistakes when you actually try
%% to load the filtered dump (which can take a very long time). We use the
%% process dictionary to gain efficiency, in particular, to store the trees
%% in a way that preserves sharing of data structures.

%% @spec cache_bin(binary()) -> binary()
%% @doc Ensures that the given binary is cached, and returns the cached
%% copy. Exported so that it may be called from filter functions.

cache_bin(Bin0) when is_binary(Bin0) ->
    %% we need to ensure that we don't store sub-binaries of temporary
    %% larger binaries in the cache, which prevents the parent binary from
    %% being garbage collected; the following trick creates a new binary
    Bin = iolist_to_binary([Bin0]),
    case get(Bin) of
        undefined -> put(Bin, Bin), Bin;
        Bin1 -> Bin1
    end.

find_node(Path, Rev) ->
    case get(Rev) of
        undefined ->
            throw({nonexisting_revision, Rev});
        Tree ->
            %% search in tree for Rev
            find_node(Path, Tree, Path, Rev)
    end.

find_node([A | As], Tree, Path0, Rev) ->
    Key = cache_bin(A),
    case gb_trees:lookup(Key, Tree) of
        {value, SubTree} when is_tuple(SubTree) ->
            find_node(As, SubTree, Path0, Rev);
        {value, File} ->
            if As =:= [] -> File;
               true -> throw({not_a_directory, A, As, Path0, Rev})
            end;
        none ->
            throw({no_such_file_or_directory, A, As, Path0, Rev})
    end;
find_node([], Tree, _, _) ->
    Tree.

%% note that you shouldn't do this with any revisions other than the latest
modify_path([], Kind, Action, Rev, _MD5) ->
    throw({modifying_empty_path, Kind, Action, Rev});
modify_path(Path, Kind, Action, Rev, MD5) when is_integer(Rev) ->
    case get(Rev) of
        undefined ->
            throw({nonexisting_revision, Rev});
        Tree ->
            try modify_path_1(Path, Kind, Action, MD5, Tree) of
                NewTree ->
                    put(Rev, NewTree)
            catch
                Throw ->
                    throw({modify_path_failed,
                           {Throw, Path, Kind, Action, Rev}})
            end
    end.

modify_path_1([A], Kind, Action, MD5, Tree) ->
    Key = cache_bin(A),
    case gb_trees:lookup(Key, Tree) of
        {value, _} ->
            %% Tree already has an entry for A
            case Action of
                add -> throw(target_exists);
                _ ->
                    %% note that add-with-history seems to be considered as
                    %% a variant of replace for existing entries
                    update_tree(A, Kind, Action, MD5, Tree)
            end;
        none ->
            %% No entry for A in Tree
            case Action of
                change -> throw(missing_target);
                replace -> throw(missing_target);
                delete -> throw(missing_target);
                _ ->
                    %% add or add-with-history both allowed for new entries
                    update_tree(A, Kind, Action, MD5, Tree)
            end
    end;
modify_path_1([A|As], Kind, Action, MD5, Tree) ->
    %% A is an intermediate directory in the path; it must have an entry
    %% in Tree, or something is wrong!
    Key = cache_bin(A),
    case gb_trees:lookup(Key, Tree) of
        {value, SubTree} when is_tuple(SubTree) ->
            %% recurse into the subtree, then update the current tree
            NewSubTree = modify_path_1(As, Kind, Action, MD5, SubTree),
            gb_trees:update(Key, NewSubTree, Tree);
        {value, _File} ->
            throw({not_a_directory, A, As});
        none ->
            %% no entry for A in its parent directory
            throw({missing_directory, A, As})
    end.

update_tree(A, _Kind, delete, _MD5, Tree) ->
    gb_trees:delete(cache_bin(A), Tree);
update_tree(A, _Kind, {add, FromPath, FromRev, FromMD5}, MD5, Tree) ->
    %% also verifies existence of copy source (exception if not found)
    case find_node(split_path(FromPath), FromRev) of
        TreeCopy when is_tuple(TreeCopy) ->
            if MD5 =/= undefined ->
                    throw({directory_copy_has_md5, A, MD5});
               true ->
                    gb_trees:enter(cache_bin(A), TreeCopy, Tree)
            end;
        FromMD5 when MD5 =:= undefined ->
            %% a plain copy that preserves the original's MD5 sum
            gb_trees:enter(cache_bin(A), cache_bin(FromMD5), Tree);
        FromMD5 ->
            %% this happens when a copy operation simultaneously inserts
            %% text content with a different MD5 sum; the source MD5 is just
            %% used for checking the connection backwards in this case
            gb_trees:enter(cache_bin(A), cache_bin(MD5), Tree);
        FileCopy ->
            throw({md5_mismatch, FromMD5, FileCopy})
    end;
update_tree(A, Kind, change, MD5, Tree) ->
    case Kind of
        dir ->
            %% just a directory property change; do nothing
            Tree;
        file when MD5 =:= undefined ->
            %% just a file property change; do nothing
            Tree;
        file ->
            %% enter new MD5
            %% TODO: we don't yet handle text-deltas and delta-md5
            gb_trees:enter(cache_bin(A), cache_bin(MD5), Tree)
    end;
update_tree(A, Kind, _Action, MD5, Tree) ->
    %% replace or add new entry
    What = case Kind of
               dir ->
                   gb_trees:empty();
               file ->
                   cache_bin(MD5)
           end,
    gb_trees:enter(cache_bin(A), What, Tree).


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
