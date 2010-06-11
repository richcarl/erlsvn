%% ---------------------------------------------------------------------
%% File: svndump.hrl
%%
%% @author Richard Carlsson <carlsson.richard@gmail.com>
%% @copyright 2010 Richard Carlsson

-record(version, {number}).
-record(uuid, {id}).
-record(revision, {number, properties=[], changes}).
-record(change, {path, kind, action, headers=[], properties=[], data}).
