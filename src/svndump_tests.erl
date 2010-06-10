%% ---------------------------------------------------------------------
%% File: svndump_tests.erl
%%
%% @author Richard Carlsson <carlsson.richard@gmail.com>
%% @copyright 2010 Richard Carlsson
%% @doc 

-module(svndump_tests).

-include_lib("eunit/include/eunit.hrl").

scan_records_file_test() ->
    {ok, Bin} = file:read_file("priv/example.dump"),
    svndump:scan_records(Bin).

scan_records_with_properties_test() ->
    Data = <<"SVN-fs-dump-format-version: 2\n\nUUID: ABC123\n\n"
	    "Revision-number: 123\nProp-content-length: 80\n"
	    "Content-length: 80\n\n"
	    "K 6\nauthor\nV 7\nsussman\nK 3\nlog\nV 33\n"
	    "Added two files, changed a third.\nPROPS-END\n\n">>,
    [{[{svn_fs_dump_format_version,2}], none, <<>>},
     {[{uuid,<<"ABC123">>}], none, <<>>},
     {[{revision_number,123},
       {prop_content_length,80},
       {content_length,80}],
      [{<<"author">>, <<"sussman">>},
       {<<"log">>, <<"Added two files, changed a third.">>}],
       <<>>}] = svndump:scan_records(Data).

scan_records_test() ->
    Data0 = <<"SVN-fs-dump-format-version: 2\n\nUUID: ABC123\n\n"
             "Revision-number: 123\nContent-length: 10\n\n0123456789\n">>,
    [{[{svn_fs_dump_format_version,2}], none, <<>>},
     {[{uuid,<<"ABC123">>}], none, <<>>},
     {[{revision_number,123}, {content_length,10}],
      none, <<"0123456789">>}] = svndump:scan_records(Data0).
