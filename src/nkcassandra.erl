%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc NkCASSANDRA application

-module(nkcassandra).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([get_client/1, query/2, query_async/2, query_async_wait/2]).
-export([has_more_pages/1, fetch_more/1, fetch_more_async/1]).
-export([size/1, head/1, head/2, tail/1, next/1, all_rows/1, all_rows/2]).

-include_lib("cqerl/include/cqerl.hrl").

%% ===================================================================
%% Types
%% ===================================================================

-type cluster_id() :: atom().
-type client() :: term().
-type sql() :: string() | binary().
-type result() :: #cql_result{}.
-type query_tag() :: reference().
-type proplist() :: lists:proplist().

%% ===================================================================
%% Types
%% ===================================================================

%% @doc
-spec get_client(cluster_id()) ->
    {ok, client()} | {error, term()}.

get_client(Cluster) ->
    cqerl:get_client(Cluster).


%% @doc
-spec query(client(), sql()) ->
    {ok, result()} | {error, term()}.

query(Client, SQL) ->
    cqerl:run_query(Client, SQL).


%% @doc
-spec query_async(client(), sql()) ->
    query_tag().

query_async(Client, SQL) ->
    cqerl:send_query(Client, SQL).


%% @doc
-spec query_async_wait(query_tag(), pos_integer()) ->
    term().

query_async_wait(Tag, Timeout) ->
    receive
        {result, Tag, Result} ->
            {ok, Result};
        {error, Tag, Error} ->
            {error, Error}
    after Timeout ->
        {error, Timeout}
    end.


%% @doc Check to see if there are more result available
-spec has_more_pages(result()) ->
    true | false.

has_more_pages(Result) ->
    cqerl:has_more_pages(Result).


%% @doc Fetch the next page of result from Cassandra for a given continuation. The function will
%%            return with the result from Cassandra (synchronously).
-spec fetch_more(result()) ->
    no_more_result | {ok, result()}.

fetch_more(Result) ->
    cqerl:fetch_more(Result).


%% @doc Asynchronously fetch the next page of result from cassandra for a given continuation.
-spec fetch_more_async(result()) ->
    query_tag() | no_more_result.

fetch_more_async(Result) ->
    cqerl:fetch_more_async(Result).


%% @doc The number of rows in a result set
-spec size(result()) ->
    integer().

size(Result) ->
    cqerl:size(Result).


%% @doc Returns the first row of result, as a property list
-spec head(result()) ->
    empty_dataset | proplist().

head(Result) ->
    cqerl:head(Result).


%% @doc Returns the first row of result, as a property list
-spec head(result(), list()) ->
    empty_dataset | proplist().

head(Result, Opts) ->
    cqerl:head(Result, Opts).



%% @doc Returns all rows of result, except the first one
-spec tail(result()) ->
    empty_dataset | result().

tail(Result) ->
    cqerl:tail(Result).


%% @doc Returns a tuple of <code>{HeadRow, ResultTail}</code>.
%% This can be used to iterate over a result set efficiently. Successively
%% call this function over the result set to go through all rows, until it
%% returns the <code>empty_dataset</code> atom.
-spec next(result()) ->
    empty_dataset | {Head::proplist(), Tail::result()}.

next(Result) ->
    cqerl:next(Result).


%% @doc Returns a list of rows as property lists
-spec all_rows(result()) ->
    [proplist()].

all_rows(Result) ->
    cqerl:all_rows(Result).


%% @doc Returns a list of rows as property lists
-spec all_rows(result(), list()) ->
    [proplist()].

all_rows(Result, Opts) ->
    cqerl:all_rows(Result, Opts).

