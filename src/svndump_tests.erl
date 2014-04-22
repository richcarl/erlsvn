%% ---------------------------------------------------------------------
%% Licensed under the Apache License, Version 2.0 (the "License"); you may
%% not use this file except in compliance with the License. You may obtain
%% a copy of the License at <http://www.apache.org/licenses/LICENSE-2.0>
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% Alternatively, you may use this file under the terms of the GNU Lesser
%% General Public License (the "LGPL") as published by the Free Software
%% Foundation; either version 2.1, or (at your option) any later version.
%% If you wish to allow use of your version of this file only under the
%% terms of the LGPL, you should delete the provisions above and replace
%% them with the notice and other provisions required by the LGPL; see
%% <http://www.gnu.org/licenses/>. If you do not delete the provisions
%% above, a recipient may use your version of this file under the terms of
%% either the Apache License or the LGPL.
%%
%% @author Richard Carlsson <carlsson.richard@gmail.com>
%% @copyright 2010 Richard Carlsson
%% @hidden

-module(svndump_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/svndump.hrl").

filter_test() ->
    Fun = fun (Rec=#change{properties = undefined}, State) ->
		  Ps = [{<<"svn:secret">>,<<"ahooga">>}],
		  {true, Rec#change{properties = Ps}, State};
	      (Rec=#change{properties = Ps}, State) ->
		  Ps1 = Ps ++ [{<<"svn:secret">>,<<"blahonga">>}],
		  {true, Rec#change{properties = Ps1}, State};
              (_Rec, State) ->
		  {true, State}
	  end,
    svndump:filter("priv/example.dump", Fun, []).

fold_test() ->
    Fun = fun (_Rec=#revision{}, N) ->
                  N + 1;
	      (_Rec, N) ->
                  N
	  end,
    ?assertEqual(2, svndump:fold("priv/example.dump", Fun, 0)).

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
