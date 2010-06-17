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
  Acc) ->
    {true, R, Acc};
f(#revision{number=_Rev, properties=_Ps}=R, Acc) ->
    {true, R, Acc+1};  % count revisions in Acc
f(#uuid{id=_UUID}=_R, Acc) ->
    {true, Acc};
f(#version{number=_Version}=_R, Acc) ->
    {true, Acc}.
