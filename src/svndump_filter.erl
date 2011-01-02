%% =====================================================================
%% This library is free software; you can redistribute it and/or modify
%% it under the terms of the GNU Lesser General Public License as
%% published by the Free Software Foundation; either version 2 of the
%% License, or (at your option) any later version.
%%
%% This library is distributed in the hope that it will be useful, but
%% WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
%% Lesser General Public License for more details.
%%
%% You should have received a copy of the GNU Lesser General Public
%% License along with this library; if not, write to the Free Software
%% Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
%% USA
%%
%% @author Richard Carlsson <carlsson.richardc@gmail.com>
%% @copyright 2010 Richard Carlsson
%% @doc Full filter example/skeleton for use with svndump. This is a
%% stripped-down version of a filter used on a real production repository.
%% For more information, look at the code.

%% NOTE: The points in the code that you typically want to modify are
%% tagged with CUSTOM. Start by looking at these, commenting out everything
%% you don't want to be done, and then adding your own rules.

-module(svndump_filter).

-include("../include/svndump.hrl").

-export([run/1]).  % API

%% @spec run(Dumpfile::string()) -> integer()
run(Dumpfile) ->
    svndump:filter(Dumpfile, fun f/2, 0).

-define(LAST_CVS, 4156).  %% if you have some cvs2svn conversion artifacts

%% Change this to to true for debugging purposes; suppresses all output,
%% yielding a fairly large speedup
-define(DRYRUN, false).

%% Change this to to true for debugging purposes; drops all content data
%% from the output, making it easier to inspect (also yields slight speedup)
-define(NODATA, false).

%% Change this to false to preserve and check consistency of existing MD5
%% headers (also saves a little CPU time if you're not modifying any content
%% anyway); if true, all MD5 sum headers will be recomputed.
-define(UPDATEMD5, true).

%% Change this to true to preserve existing mergeinfo properties, only
%% updating the source paths; if false, all mergeinfo will be replaced.
-define(KEEPMERGEINFO, false).

%% NOTE: Since we know that this function will be run by the svndump server
%% process, we can abuse the process dictionary quite a lot to gain speed.

%% kind: file | dir | undefined  (optional for a delete action)
%% action: change | add | {add, Path, Rev, MD5} | delete | replace

%% @spec f(Record::term(), Accumulator::integer()) -> integer()

f(#change{action=add, path = <<"web/trunk">>}, Acc) ->
    %% CUSTOM: pretend this add never happened
    {false, Acc};
f(#change{action=Action0, kind=_Kind, path=Path0, headers=Hs,
          properties=Ps, data=Data0}=R,
  Acc) ->
    Data = if ?NODATA =:= true -> <<>>;
              true -> Data0
           end,
    #revision{number=Rev} = get(revision),

    %% CUSTOM: fixes for some very weird branching attempts:
    Action = case Rev of
                 24298 -> {add, <<"trunk/frob">>, Rev-1, undefined};
                 24880 -> {add, <<"trunk/frob">>, Rev-1, undefined};
                 27078 -> {add, <<"trunk/frob">>, Rev-1, undefined};
                 _ -> Action0
             end,

    SplitPath0 = split_path(Path0),
    case fix_path(SplitPath0) of
        [] ->
            {false, Acc};  % drop changes for which fix_path returns []
        Path ->
            %% Check that the remapped path is OK
            check_allowed_path(Path, Rev, Action),

            %% Note that renamings may turn an add-with-history into a copy
            %% from some Path to the same Path, but (apparently) it's no
            %% problem to svn if the target path already exists (and some
            %% Path->Path copies may be resurrects, and must not be lost).
            %% Add without history must however not use an existing path;
            %% in that case, it must be changed to a 'replace'.
            Action1 = case Action of
                          {add, FromPath0, FromRev, FromMD5} ->
                              %% Fix branch points (based on the paths
                              %% before renamings) and remap the from-path:
                              SplitFromPath0 = split_path(FromPath0),
                              SplitFromPath1 = fix_frompath(SplitFromPath0,
                                                            SplitPath0, Rev),
                              case fix_path(SplitFromPath1) of
                                  [] ->
                                      exit({empty_frompath, Rev});
                                  FromPath ->
                                      check_allowed_path(FromPath, Rev,
                                                         Action),
                                      %% Check for bad branchings:
                                      check_branch(Path, FromPath, Rev),
                                      {add, FromPath, FromRev, FromMD5}
                              end;
                          _ ->
                              Action
                      end,
            %% Fix up mergeinfo properties (depending on settings):
            Ps1 = fix_properties(Ps, Action1, Path),

            %% The new change record:
            R1 = R#change{action=Action1,
                          path=Path,
                          properties=Ps1,
                          headers=drop_sha1(Hs),
                          data=Data},

            %% Some branches may need to be performed in several places due
            %% to path rewrites:
            maybe_dup_branch(R1),

            %% Is this change one of the deletes we need to drop?
            case (Action1 =:= delete) andalso (drop_delete(Path, Rev)) of
                true ->
                    {false, Acc};
                false ->
                    %% Track the merge information
                    case Action1 of
                        delete ->
                            maybe_forget_merges(Rev, Path);
                        _ ->
                            maybe_copy_mergeinfo(Rev, Path, Action1),
                            remember_path(Path)
                    end,
                    {true, add_extra(R1), Acc}
            end
    end;
f(#revision{number=Rev, properties=Ps}=R, Acc) when is_integer(Rev) ->
    #revision{number=PrevRev} = get(revision),

    maybe_add_mergeinfo(PrevRev),  % emit mergeinfo changes before new rev

    %% copy mergeinfo to new revision
    Revs = get(mergeinfo),
    put(mergeinfo, array:set(Rev, array:get(PrevRev, Revs), Revs)),

    put(revision, R),  % keep revision record for processing changes

    Acc1 = Acc+1, % count revisions in Acc (not terribly useful, but...)

    Ps1 = fix_logs(Rev, Ps),  % rewrite some log messages

    %% output progress information
    if (Rev rem 1000) =:= 0 ->
            io:format("Revision-number: ~p.\n", [Rev]);
       true ->
            ok
    end,

    %% add the accumulated extra changes to this revision
    Rs = add_extra(R#revision{properties = Ps1}),

    %% create some (otherwise missing) directories in the first revision
    if Rev =:= 1 ->
            {true, Rs ++ extra_dirs(), Acc1};
       true ->
            {true, Rs, Acc1}
    end;
f(#uuid{id=UUID}=_R, Acc) ->
    io:format("UUID: ~p.\n", [UUID]),
    {true, Acc};
f(#version{number=Version}=_R, Acc) ->
    %% This should be the first record - initialize your process state here!
    io:format("Dump format version: ~p.\n", [Version]),
    put(revision, #revision{number=0}),
    put(dry_run, ?DRYRUN),
    put(update_md5, ?UPDATEMD5),
    put(keep_mergeinfo, ?KEEPMERGEINFO),
    put(mergeinfo, array:set(0, dict:new(), array:new())),
    {ok, RE} = re:compile("[^-]*-r[ ]*\([0-9]\+\):\([0-9]\+\)[^:]\+://[^/]\+/\(?:repos/\)\?frobnitz/\([^ \t\r\n]*\).*"),  % CUSTOM
    put(log_re, RE),
    reset_extra(),
    {true, Acc}.

split_path(P) ->
    re:split(P, "/").

get_remembered_paths() ->
    sets:to_list(get(paths)).

reset_remembered_paths() ->
    put(paths, sets:new()).

remember_path(P) ->
    put(paths, sets:add_element(P, get(paths))).

get_log_message([{<<"svn:log">>, Text} | _]) -> Text;
get_log_message([_ | Ps]) -> get_log_message(Ps);
get_log_message([]) -> undefined.

set_log_message(Rev, Text, [{<<"svn:log">>, _} | Ps]) ->
    Log = iolist_to_binary("r" ++ integer_to_list(Rev) ++ ": " ++ Text),
    [{<<"svn:log">>, Log} | Ps];
set_log_message(Rev, Text, [P | Ps]) ->
    [P | set_log_message(Rev, Text, Ps)].

fix_logs(Rev, Ps) ->
    case get_log_message(Ps) of
        undefined ->
            put(merge, undefined),
            Ps;
        Text0 when is_binary(Text0) ->
            %% CUSTOM: correct some weird log messages
            Text = case Text0 of
                       <<"\"\"">>   -> <<"(no message)">>;  % ""
                       <<"\"\"\n">> -> <<"(no message)">>;  % ""\n
                       <<"*** empty log message ***\n">> -> <<"(no message)">>;
                       X -> X
                   end,

            %% CUSTOM:
            %% Normalize log messages created by various incarnations of a
            %% home-made svn merge script, and translate to real mergeinfo
            RE = get(log_re),
            case re:run(Text, RE,[{capture,all_but_first,binary}]) of
                nomatch ->
                    put(merge, undefined),
                    set_log_message(Rev, Text, Ps);
                {match, [FromRev,ToRev,Path]} ->
                    %% drop the base path from the merge source; only
                    %% trunk/... or branches/... are allowed as sources
                    Path1 = case fix_path(split_path(Path)) of
                                [_Base | [<<"trunk">>]=P] -> P;
                                [_Base | [<<"branches">>, _B]=P] -> P;
                                P -> exit({bad_merge_source, P})
                            end,
                    put(merge, {list_to_integer(binary_to_list(FromRev)),
                                list_to_integer(binary_to_list(ToRev)),
                                Path1}),
                    Log = iolist_to_binary(["merge -r", FromRev, ":", ToRev,
                                            " ", svndump:join_path(Path1)]),
                    set_log_message(Rev, Log, Ps)
            end
    end.

%% some infrastructure for inserting extra changes after the current
reset_extra() ->
    put(pre, []),
    put(post, []).

add_post(R) ->
    put(post, [R | get(post)]).

add_pre(R) ->
    put(pre, [R | get(pre)]).

%% this adds all the accumulated changes
add_extra(R) ->
    Rs = get(pre) ++ [R] ++ get(post),
    reset_extra(),
    Rs.

%% These sha1-properties won't be automatically updated by svndump.erl, so
%% just drop them if they occur, and hope there are md5 properties as well.
drop_sha1([{text_delta_base_sha1, _} | Hs]) -> drop_sha1(Hs);
drop_sha1([{text_copy_source_sha1, _} | Hs]) -> drop_sha1(Hs);
drop_sha1([{text_content_sha1, _} | Hs]) -> drop_sha1(Hs);
drop_sha1([H | Hs]) -> [H | drop_sha1(Hs)];
drop_sha1([]) -> [].

%% CUSTOM: Extra directories that need to be created in revision 1
extra_dirs() ->
    [begin
         #change{action=add, kind=dir, path=Dir}
     end
     || Dir <- [<<"web">>,
                <<"web/trunk">>,
                <<"web/tags">>,
                <<"web/branches">>
               ]].

fix_properties(Ps, _Action, _Path) ->
    case get(keep_mergeinfo) of
        true ->
            fix_mergeinfo(Ps);
        false ->
            no_mergeinfo(Ps)
    end.

%% Update mergeinfo source paths
fix_mergeinfo([{<<"svn:mergeinfo">>, []} | Ps]) ->
    fix_mergeinfo(Ps);  % drop empty mergeinfo entries
fix_mergeinfo([{<<"svn:mergeinfo">>, Merges0} | Ps]) ->
    %% make sure to drop killed paths
    Merges = [M || {P, _}=M <- [fix_mergeinfo_path(M0) || M0 <- Merges0],
                   P =/= [<<"">>]],
    [{<<"svn:mergeinfo">>, Merges} | fix_mergeinfo(Ps)];
fix_mergeinfo([P | Ps]) ->
    [P | fix_mergeinfo(Ps)];
fix_mergeinfo([]) ->
    [];
fix_mergeinfo(undefined) ->
    undefined.

fix_mergeinfo_path({Path0, Ranges}) ->
    %% These paths start with a slash (Subversion is pretty inconsistent),
    %% so we always get an empty initial segment that must be ignored when
    %% remapping the paths, and must then be put back again... sigh...
    [<<"">> | Path] = split_path(Path0),
    {[<<"">> | fix_path(Path)], Ranges}.

no_mergeinfo([{<<"svn:mergeinfo">>, _} | Ps]) ->
    no_mergeinfo(Ps);
no_mergeinfo([P | Ps]) ->
    [P | no_mergeinfo(Ps)];
no_mergeinfo([]) ->
    [];
no_mergeinfo(undefined) ->
    undefined.

%% Called when a new revision record is seen, but before the current
%% revision information has been updated:
maybe_add_mergeinfo(CurrRev) ->
    case get(merge) of
        {FromRev, ToRev, FromPath} ->
            %% Record this merge, for each root path involved in the
            %% revision.
            lists:foreach(fun (Target) ->
                                  add_mergeinfo(Target, CurrRev,
                                                FromRev, ToRev, FromPath)
                          end,
                          roots(get_remembered_paths()));
        undefined ->
            ok
    end,
    reset_remembered_paths().

add_mergeinfo([Base|_]=Target, CurrRev, FromRev, ToRev, FromPath) ->
    %% The from-path is given without base (to make it independent of
    %% project), so the base of the target must be prepended. An empty
    %% segment must also be prepended so that the paths get a leading slash.
    FromPath1 = [<<"">>, Base] ++ FromPath,
    MergeInfo = update_merges(CurrRev, Target, FromPath1, FromRev, ToRev),
    add_pre(#change{action=change, kind=dir,
                    path=Target,
                    properties=[{<<"svn:mergeinfo">>, MergeInfo}]
                   }).

roots(Paths) ->
    ordsets:from_list([root(P) || P <- Paths]).

root([Base, <<"trunk">> | _]) -> [Base, <<"trunk">>];
root([Base, <<"branches">>, B | _]) -> [Base, <<"branches">>, B];
root([Base, <<"tags">>, T | _]) -> [Base, <<"tags">>, T];
root([Base]) -> [Base];
root([]) -> [].

%% Avoid storing path strings more than once
cache_path(P) ->
    svndump:cache_bin(svndump:join_path(P)).

update_merges(CurrRev, Target0, Source0, FromRev, ToRev)
  when FromRev =< ToRev ->
    Target = cache_path(Target0),
    Source = cache_path(Source0),
    Revs = get(mergeinfo),
    Targets = array:get(CurrRev, Revs),
    Sources = case dict:find(Target, Targets) of
                  {ok, S} -> S;
                  error -> dict:new()
              end,
    %% We store the range as a singleton list, so we can use the result from
    %% dict:to_list(Sources) directly as a mergeinfo property value
    Sources1 = case dict:find(Source, Sources) of
                   {ok, [{Start, End}]} when End =< ToRev ->
                       %% We silently fill in any holes in the merge history
                       dict:store(Source, [{Start, ToRev}], Sources);
                   {ok, [{Start, End}]} ->
                       io:format("Weird merge: ~p, ~p, ~p, ~p.\n",
                                 [Target, Source, {Start, End},
                                  {FromRev, ToRev}]),
                       dict:store(Source, [{Start, End}], Sources);
                   error ->
                       dict:store(Source, [{FromRev, ToRev}], Sources)
               end,
    Targets1 = dict:store(Target, Sources1, Targets),
    put(mergeinfo, array:set(CurrRev, Targets1, Revs)),
    dict:to_list(Sources1).

maybe_forget_merges(CurrRev, Target0) ->
    case root(Target0) of
        Target0 ->
            Target = cache_path(Target0),
            %% A root path was deleted, so erase all its mergeinfo
            Revs = get(mergeinfo),
            Targets = array:get(CurrRev, Revs),
            Targets1 = dict:erase(Target, Targets),
            put(mergeinfo, array:set(CurrRev, Targets1, Revs));
        _ ->
            ok
    end.

maybe_copy_mergeinfo(CurrRev, Target0, {add, Source0, FromRev, _MD5}) ->
    %% Get mergeinfo for Source in FromRev and copy to Target in CurrRev
    case {root(Target0), root(Source0)} of
        {Target0, Source0} ->
            Target = cache_path(Target0),
            Source = cache_path(Source0),
            Revs = get(mergeinfo),
            Targets = array:get(FromRev, Revs),
            case dict:find(Source, Targets) of
                {ok, Sources} ->
                    Targets1 = dict:store(Target, Sources, Targets),
                    put(mergeinfo, array:set(CurrRev, Targets1, Revs));
                error ->
                    ok  % nothing to copy
            end;
        _ ->
            ok  % not a branch operation
    end;
maybe_copy_mergeinfo(_CurrRev, _Target, _Action) ->
    ok.

%% Paths are remapped in multiple stages, and finally checked
fix_path(Path) ->
    map_vendor(map_path(Path)).

%% CUSTOM: Moves vendor branches into 'branches' directory, prefixed with
%% 'vendor-' (makes it easier for a subsequent svn-to-git conversion)
map_vendor([_, <<"vendor">>]) ->
    [];  % kill (drop the 'vendor' directory itself)
map_vendor([Base, <<"vendor">>, Dir | R]) ->
    [Base, <<"branches">>, <<"vendor-", Dir/bytes>> | R];
map_vendor(Path) ->
    Path.

%% CUSTOM: Check that paths (after renaming) follow your new conventions
check_allowed_path(Path, Rev, Action) ->
    case Path of
        [_, <<"tags">> | _] when Rev =< ?LAST_CVS ->
            ok;  % allow all (tags made by cvs2svn)
        [_Base, <<"tags">>, _T, _ | _] ->
            %% no changes allowed below directories of tags
            exit({tag_change, Path});
        [_Base, <<"tags">> | _] ->
            ok;
        [_Base, <<"branches">>, B]
        when Action =:= add ; Action =:= replace ->
            io:format("Warning: ~w of branches/~s in ~p! (~p)\n",
                      [Action, B, Rev, Path]),
            ok;
        [_Base, <<"branches">> | _] ->
            ok;
        [_Base, <<"trunk">> | _] ->
            ok;
        [_Base] ->
            ok;
        _ ->
            exit({disallowed_path, Path, Rev, Action})
    end.

%% CUSTOM: Special from-path fixes (before the path rewrites)
fix_frompath([<<"trunk">>], _Path, _Rev) ->
    %% partly because of the need to map trunk to frob/branches below
    [<<"trunk">>, <<"frob">>];
fix_frompath(FromPath, _Path, _Rev) ->
    FromPath.

%% Check for bad branchings/taggings (final paths are used here!)
check_branch(Path, [], _Rev) ->
    exit({branch_source_killed, Path});
check_branch(Path, FromPath, Rev) ->
    case Path of
        FromPath ->
            ok;  % special case (redundant), sometimes generated by renaming
        [_, <<"branches">>, _, _ | _]  when Rev =< ?LAST_CVS ->
            ok;  % allow all (special case due to cvs2svn)
        [_, <<"tags">>, _, _ | _]  when Rev =< ?LAST_CVS ->
            ok;  % allow all (special case due to cvs2svn)
        [Base, Type, _] when Type =:= <<"tags">> ; Type =:= <<"branches">> ->
            case FromPath of
                [Base, <<"trunk">>] -> ok;
                [Base, <<"tags">>, _] -> ok;
                [Base, <<"branches">>, _] -> ok;
                _ -> exit({bad_copy, Path, FromPath})
            end;
        [Base, <<"trunk">>] ->
            %% allow creating trunk from a vendor branch
            case FromPath of
                [Base, <<"branches">>, <<"vendor-", _/bytes>>] -> ok;
                _ -> exit({bad_copy, Path, FromPath})
            end;
        [Base, <<"trunk">>, _ | _] ->
            case FromPath of
                [Base, <<"trunk">>, _ | _] -> ok;
                [Base, <<"branches">>, _B, _ | _] -> ok;
                _ -> exit({bad_copy, Path, FromPath})
            end;
        [Base, <<"branches">>, _B, _ | _] ->
            case FromPath of
                [Base, <<"trunk">>, _ | _] -> ok;
                [Base, <<"branches">>, _B1, _ | _] -> ok;
                _ -> exit({bad_copy, Path, FromPath})
            end;
        _ ->
            exit({bad_copy, Path, FromPath})
    end.

%% CUSTOM: Some deletes must be dropped due to renamings (note that final
%% paths are used here!)
drop_delete(Path, Rev) ->
    case Path of
        [<<"frob">>] -> true;
        [<<"frob">>, <<"trunk">>] -> true;
        [<<"frob">>, <<"tags">>] -> true;
        [<<"frob">>, <<"branches">>] -> true;
        [<<"frob">>, <<"branches">>, <<"devel">>] -> true;
        [<<"frob">>, <<"branches">>, <<"rudolf-csv">>] -> true;

        [<<"frob">>, <<"branches">>, <<"alarm">>]
        when Rev =:= 24740 -> true;  % already deleted due to renamings

        [<<"frob">>, <<"branches">>, _B]
        when Rev >= 24740, Rev =< 24784 -> true;  % repo restructuring

        [<<"web">>, <<"trunk">>, <<"php">>]
        when Rev < 28434 -> true;

        _Other -> false
    end.

%% CUSTOM: Certain branches/tags must be done in multiple places due to
%% renamings, typically when you have broken out a set of files into a
%% separate project with its own trunk/branches/tags
maybe_dup_branch(#change{action={add, [<<"frob">> | FromPathRest],
                                 FromRev, FromMD5},
                         path=[<<"frob">>, Type, B]}=R)
  when Type =:= <<"tags">> ; Type =:= <<"branches">>->
    if 483 < FromRev, FromRev < 28434 ->
            add_post(R#change{action={add, [<<"web">>
                                            | FromPathRest],
                                      FromRev, FromMD5},
                              path=[<<"web">>, Type, B]});
       true ->
            ok
    end;
maybe_dup_branch(_R) ->
    ok.

%% CUSTOM: Maps original paths to new paths; also checks for allowed paths
%% in general. This is typically where most of your work gets done.
%% Returning an empty list causes the entire change to be dropped.
map_path(Path) ->
    case Path of
        %% -----------------------------------------------------------
        %% Paths under the old layout (before our repo reorganization)
        %% -----------------------------------------------------------

        %% 'branches' was the first created directory in our svn history,
        %% so we'll use that as the 'frob' directory in the new layout
        [<<"branches">>] ->
            [<<"frob">>];

        %% then the 'tags' directory from the old layout can be remapped to
        %% the new layout, since we know that 'frob' exists
        [<<"tags">>] ->
            [<<"frob">>, <<"tags">>];

        %% make the directory originally created as 'trunk' be the
        %% 'frob/branches' directory in the new layout instead
        [<<"trunk">>] ->
            [<<"frob">>, <<"branches">>];


        %% pretend that the php stuff was always under its own project
        [<<"trunk">>, <<"frob">>, <<"lib">>, <<"php">> | R] ->
            [<<"web">>, <<"trunk">>, <<"php">> | R];
        [<<"tags">>, T, <<"frob">>, <<"lib">>, <<"php">> | R] ->
            [<<"web">>, <<"tags">>, T, <<"php">> | R];
        [<<"branches">>, B, <<"frob">>, <<"lib">>, <<"php">> | R] ->
            [<<"web">>, <<"branches">>, B, <<"php">> | R];
        [<<"branches">>, B, <<"lib">>, <<"php">> | R] ->
            %% some branches got created without the extra 'frob' directory
            [<<"web">>, <<"branches">>, B, <<"php">> | R];


        %% get rid of a huge file that shouldn't have been checked in
        [<<"trunk">>, <<"frob">>, <<"lib">>, <<"baz">>,
         <<"priv">>, <<"foo.dets">>] ->
            [];  % kill
        [<<"tags">>, _T, <<"frob">>, <<"lib">>, <<"baz">>,
         <<"priv">>, <<"foo.dets">>] ->
            [];  % kill
        [<<"branches">>, _B, <<"frob">>, <<"lib">>, <<"baz">>,
         <<"priv">>, <<"foo.dets">>] ->
            [];  % kill


        %% remove directory of large files, should not have been checked in
        [<<"trunk">>, <<"frob">>, <<"blobs">> | _R] ->
            [];  % kill
        [<<"tags">>, _T, <<"frob">>, <<"blobs">> | _R] ->
            [];  % kill
        [<<"branches">>, _B, <<"frob">>, <<"blobs">> | _R] ->
            [];  % kill


        %% remove artifacts from the bad old CVS days
        [<<"trunk">>, <<"CVSROOT">> | _R] ->
            [];  % kill
        [<<"tags">>, _T, <<"CVSROOT">> | _R] ->
            [];  % kill
        [<<"branches">>, _B, <<"CVSROOT">> | _R] ->
            [];  % kill


        %% get rid of weird tags and branches created by cvs2svn
        [<<"tags">>, <<"start">> | _R] ->
            [];  % kill
        [<<"tags">>, <<"frob">> | _R] ->
            [];  % kill
        [<<"tags">>, <<"FROB-4-4">> | _R] ->
            [];  % kill
        [<<"branches">>, <<"vend">> | _R] ->
            [];  % kill


        %% rules for specific branches (in the old layout)

        [<<"branches">>, <<"rudolf-merge">> | R] ->
            %% was copied directly from 'trunk/frob', not from 'trunk'
            [<<"frob">>, <<"branches">>, <<"rudolf-merge">> | R];

        [<<"branches">>, <<"rm_frobozz_record">>, <<"frob">> | R] ->
            %% this was copied from its own parent directory in order to
            %% get proper structure - rename it to avoid name overlap
            [<<"frob">>, <<"branches">>, <<"rm_frobozz_record_2">>
             | R];

        [<<"branches">>, <<"add_frobozz_flags">> | R] ->
            %% was copied directly from 'branches/rm_frobozz_record'
            [<<"frob">>, <<"branches">>, <<"add_frobozz_flags">> | R];

        [<<"branches">>, <<"frobinstall">>] ->
            [];  % kill (was erroneously created and then deleted again)

        [<<"branches">>, <<"p102">>, <<"trunk">>] ->
            [];  % kill (was erroneously created and then deleted again)

        %% handling strange paths caused by a confused developer trying to
        %% fix a bad branch attempt - we rename these copies apart so they
        %% never overlap, otherwise conversion to git can be very confused
        [<<"branches">>, <<"rudolf-foo">>, <<"frob">>] ->
            %% erroneously created, later moved to branches/rudolf-temp
            [<<"frob">>, <<"branches">>, <<"rudolf-temp-0">>];
        [<<"branches">>, <<"rudolf-temp">>, <<"rudolf-foo">>] ->
            %% he kept moving branches around...
            [<<"frob">>, <<"branches">>, <<"rudolf-temp-1">>];
        [<<"branches">>, <<"rudolf-foo">>, <<"frob">>,
         <<"rudolf-foo">>] ->
            %% ...and around
            [<<"frob">>, <<"branches">>, <<"rudolf-temp-2">>];
        [<<"branches">>, <<"rudolf-temp">>, <<"frob">>] ->
            %% ...and around again
            [<<"frob">>, <<"branches">>, <<"rudolf-temp-3">>];
        [<<"branches">>, <<"rudolf-foo">>, <<"frob">> | R] ->
            [<<"frob">>, <<"branches">>, <<"rudolf-temp-0">> | R];
        [<<"branches">>, <<"rudolf-foo">> | R] ->
            %% copied from trunk/frob
            [<<"frob">>, <<"branches">>, <<"rudolf-foo">> | R];


        %% remap trunk, tags, and branches from old layout to new (and get
        %% rid of useless extra 'frob' directory level under 'trunk'). Some
        %% copies erroneously included the 'trunk' directory, so we have to
        %% handle those specially.
        [<<"trunk">>, <<"frob">> | R] ->
            [<<"frob">>, <<"trunk">> | R];

        [<<"tags">>, T, <<"frob">> | R] ->
            [<<"frob">>, <<"tags">>, T | R];
        [<<"tags">>, T, <<"trunk">>] ->
            [<<"frob">>, <<"tags">>, T];
        [<<"tags">>, T, <<"trunk">>, <<"frob">> | R] ->
            [<<"frob">>, <<"tags">>, T | R];

        [<<"branches">>, B, <<"frob">> | R] ->
            [<<"frob">>, <<"branches">>, B | R];
        [<<"branches">>, B, <<"trunk">>] ->
            [<<"frob">>, <<"branches">>, B];
        [<<"branches">>, B, <<"trunk">>, <<"frob">> | R] ->
            [<<"frob">>, <<"branches">>, B | R];

        [<<"tags">>, T] ->
            [<<"frob">>, <<"tags">>, T];
        [<<"branches">>, B] ->
            [<<"frob">>, <<"branches">>, B];

        %% check for other complex paths that we don't have a rule for yet
        [<<"tags">>, _T, _Other | _R]=Path ->
            exit({bad_path, Path});
        [<<"branches">>, _B, _Other | _R]=Path ->
            exit({bad_path, Path});


        %% -------------------------------------------------------------
        %% Paths already under the new layout (after the reorganization)
        %% -------------------------------------------------------------

        %% don't copy some weird tags made by cvs2svn
        [<<"frob">>, <<"tags">>, <<"start">>] ->
            [];  % kill
        [<<"frob">>, <<"tags">>, <<"frob">> | _R] ->
            [];  % kill
        [<<"frob">>, <<"tags">>, <<"FROB-4-4">> | _R] ->
            [];  % kill

        %% remove a bad branch attempt (was never used)
        [<<"frob">>, <<"branches">>, <<"noreply">>, <<"trunk">>] ->
            [];  % kill

        %% pretend that the php stuff was always under its own project
        [<<"frob">>, <<"trunk">>, <<"lib">>, <<"php">> | R] ->
            [<<"web">>, <<"trunk">>, <<"php">> | R];
        [<<"frob">>, <<"branches">>, B, <<"lib">>, <<"php">> | R] ->
            [<<"web">>, <<"branches">>, B, <<"php">> | R];
        [<<"frob">>, <<"tags">>, T, <<"lib">>, <<"php">> | R] ->
            [<<"web">>, <<"tags">>, T, <<"php">> | R];

        %% this branch was originally copied to the wrong location, and
        %% then moved into branches - make it look like it was created in
        %% the right location but under another name instead
        [<<"frob">>, <<"mickey">> | R] ->
            [<<"frob">>, <<"branches">>, <<"mickey-0">> | R];

        %% drop some directory creations that we have already got in place
        [<<"frob">>] ->
            [];  % kill
        [<<"frob">>, <<"branches">>] ->
            [];  % kill
        [<<"frob">>, <<"tags">>] ->
            [];  % kill
        [<<"web">>] ->
            [];  % kill
        [<<"web">>, <<"tags">>] ->
            [];  % kill
        [<<"web">>, <<"branches">>] ->
            [];  % kill

        [<<"frob">> | R] -> [<<"frob">> | R];  % keep the rest under 'frob'

        [<<"web">> | R] -> [<<"web">> | R];  % keep the rest under 'web'

        [<<"lhttpc">> | R] -> [<<"lhttpc">> | R];  % keep all these


        Path ->
            exit({bad_path, Path})  % report paths that we don't handle yet
    end.
