%% ---------------------------------------------------------------------
%% File: testfilter.erl
%%
%% $Id:$ 
%%
%% @author Richard Carlsson <richardc@klarna.com>
%% @copyright 2010 Richard Carlsson
%% @doc 

-module(testfilter).

-include("../include/svndump.hrl").

-export([f/2]).

%% kind: file | dir | undefined  (optional for a delete action)
%% action: change | add | delete | replace

f(#change{action=_Action, kind=_Kind, path=_Path, headers=_Hs,
          properties=_Ps, data=_Data}=R,
  St) ->
    {true, R, St};
f(#revision{number=_N, properties=_Ps}=R, St) ->
    {true, R, St};
f(#uuid{id=_UUID}=_R, St) ->
    {true, St};
f(#version{number=_Version}=_R, St) ->
    {true, St}.
