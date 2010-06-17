%% ---------------------------------------------------------------------
%% File: svndump_tests.erl
%%
%% @author Richard Carlsson <carlsson.richard@gmail.com>
%% @copyright 2010 Richard Carlsson
%% @doc 

-module(svndump_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/svndump.hrl").

transform_test() ->
    Fun = fun (Rec=#change{properties = Ps}) ->
		  Ps1 = Ps ++ [{<<"svn:secret">>,<<"blahonga">>}],
		  Rec#change{properties = Ps1};
	      (Other) ->
		  Other
	  end,
    svndump:filter_dump("priv/example.dump", Fun).


scan_records_file_test() ->
    {ok, Bin} = file:read_file("priv/example.dump"),
    svndump:scan_records(Bin).

scan_records_with_properties_test() ->
    Data = <<"SVN-fs-dump-format-version: 2\n\nUUID: ABC123\n\n"
	    "Revision-number: 123\nProp-content-length: 80\n"
	    "Content-length: 80\n\n"
	    "K 6\nauthor\nV 7\nsussman\nK 3\nlog\nV 33\n"
	    "Added two files, changed a third.\nPROPS-END\n\n">>,
    [#version{number = 2},
     #uuid{id = <<"ABC123">>},
     #revision{number = 123,
	       properties =
	       [{<<"author">>, <<"sussman">>},
		{<<"log">>, <<"Added two files, changed a third.">>}]}
    ] = svndump:scan_records(Data).

scan_records_test() ->
    Data = <<"SVN-fs-dump-format-version: 2\n\nUUID: ABC123\n\n"
	    "Revision-number: 123\nProp-content-length: 10\n"
	    "Content-length: 10\n\nPROPS-END\n\n">>,
    [#version{number = 2},
     #uuid{id = <<"ABC123">>},
     #revision{number = 123}] = svndump:scan_records(Data).
