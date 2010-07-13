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

%% The `data' field can be `undefined' for actions like `delete'.
%% The `action' field is `{add, FromPath, FromRev}' for adds with history;
%% this will be formatted as change of type add with the headers
%% Node-copyfrom-path and Node-copyfrom-rev. Note that the data field is not
%% necessarily empty for an add with history!
-record(change, {path, kind, action, properties, headers=[], md5, data}).
