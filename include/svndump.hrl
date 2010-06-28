%% ---------------------------------------------------------------------
%% File: svndump.hrl
%%
%% @author Richard Carlsson <carlsson.richard@gmail.com>
%% @copyright 2010 Richard Carlsson

-record(version, {number, headers=[]}).

-record(uuid, {id, headers=[]}).

%% Note: the `changes' field of a revision record is normally not used by
%% plain log operations, but is included to make it easy to transform a log
%% of mixed records into a set of revision records only with the changes as
%% subnodes.
-record(revision, {number, properties, headers=[], changes}).

%% The `data' field can be `undefined' for actions like `add'
-record(change, {path, kind, action, properties, headers=[], data}).
