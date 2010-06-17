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

-export([f/1]).

%% kind: file | dir | undefined  (optional for a delete action)
%% action: change | add | delete | replace

f(#change{action=_Action, kind=_Kind, path=_Path, headers=_Hs,
          properties=_Ps, data=_Data}=R) ->
    R;
f(#revision{number=_N, properties=_Ps}=R) ->
    R;
f(#uuid{id=_UUID}=R) ->
    R;
f(#version{number=_Version}=R) ->
    R.
