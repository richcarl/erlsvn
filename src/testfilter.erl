%% ---------------------------------------------------------------------
%% File: testfilter.erl
%%
%% @author Richard Carlsson <carlsson.richardc@gmail.com>
%% @copyright 2010 Richard Carlsson
%% @doc Minimal filter example for svndump.erl.

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
