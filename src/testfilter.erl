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
%% @author Richard Carlsson <carlsson.richardc@gmail.com>
%% @copyright 2010 Richard Carlsson
%% @doc Minimal filter example for svndump.erl. For details, see the code.

-module(testfilter).

-include("../include/svndump.hrl").

-export([f/2]).

%% kind: file | dir | undefined  (optional for a delete action)
%% action: change | add | delete | replace
%%
%% The accumulator is an integer count of the number of revision records
%% seen; typically initialized to 0.

%% @spec f(Record::term(), Accumulator::integer()) -> integer()

f(#change{action=_Action, kind=_Kind, path=_Path, headers=_Hs,
          properties=_Ps, data=_Data}=R,
  Acc) ->
    {true, R, Acc};
f(#revision{number=_Rev, properties=_Ps}=R, Acc) ->
    {true, R, Acc+1};  % count revisions in Acc
f(#uuid{id=_UUID}=_R, Acc) ->
    {true, Acc};
f(#version{number=_Version}=_R, Acc) ->
    {true, Acc}.
