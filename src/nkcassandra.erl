%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Carlos Gonzalez Florido.  All Rights Reserved.
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

%% @doc NkCASSANDRA API

-module(nkcassandra).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([query/2, query/3, options/1]).


-define(LLOG(Type, Txt, Args),
    lager:Type("NkCASSANDRA Client "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================

-type value() ::
    binary() | string() | atom() |
    {value_type(), term()}.

-type value_type() ::
    ascii |
    bigint |
    blob |
    boolean |
    counter |
    decimal |
    double |
    float |
    int |
    timestamp |
    uuid |
    varchar |
    varint |
    timeuuid |
    inet |
    list |
    map |
    set |
    udt |
    tuple.


-type consistency() ::
    any |
    one |
    two |
    three |
    quorum |
    all |
    local_quorum |
    each_quorum |
    serial |
    local_serial |
    local_one.


-type query_opts() ::
    #{
        values => [value()] | undefined,
        consistency => consistency(),
        span_op => binary(),                % If defined, creates a new span
        span_tags => map()
    }.


%% ===================================================================
%% Types
%% ===================================================================


%% @doc
-spec query(nkserver:id() | {nkserver:id(), pid()}, iolist()|string()) ->
    ok | {ok, binary(), list(), list()} |
    {error, nkcassandra_protocol:error()|term()}.

query(Id, Query) ->
    query(Id, Query, #{}).


%% @doc
-spec query(nkserver:id() | {nkserver:id(), pid()}, iolist()|string(), query_opts()) ->
    ok | {ok, binary(), list(), list()} |
    {error, nkcassandra_protocol:error()|term()}.

query(Id, Query, Opts) ->
    Values = maps:get(values, Opts, undefined),
    Level = maps:get(level, Opts, one),
    Span = case Opts of
        #{span_op:=Op} ->
            S0 = nkserver_ot:span(Id, Op),
            S1 = nkserver_ot:tag(S0, <<"cassandra.sql">>, Query),
            case Opts of
                #{span_tags:=Tags} ->
                    nkserver_ot:tags(S1, Tags);
                _ ->
                    S1
            end;
        _ ->
            undefined
    end,
    Result = query(Id, Query, Values, Level, 2),
    case Span of
        undefined ->
            ok;
        _ ->
            Span2 = case Result of
                ok ->
                    nkserver_ot:log(Span, <<"result OK">>);
                {ok, _} ->
                    nkserver_ot:log(Span, <<"result OK (data)">>);
                {error, Error} ->
                    S2 = nkserver_ot:log(Span, {"result ERROR: ~p", [Error]}),
                    nkserver_ot:tag_error(S2, Error)
            end,
            nkserver_ot:finish(Span2)
    end,
    Result.




%% @doc
options(Id) ->
    case nkpacket_pool:get_conn_pid(Id) of
        {ok, Pid, _} ->
            nkcassandra_protocol:options(Pid);
        {error, Error} ->
            {error, Error}
    end.




%% ===================================================================
%% Internal
%% ===================================================================

%% @private
query({SrvId, Pid}, Query, Values, Level, Tries) when is_atom(SrvId), is_pid(Pid), Tries > 0 ->
    Debug = nkserver:get_cached_config(SrvId, nkcassandra, debug),
    case Debug of
        true ->
            ?LLOG(debug, "QUERY: ~s", [Query]);
        _ ->
            ok
    end,
    Result = nkcassandra_protocol:query(Pid, Query, Values, Level),
    case Debug of
        true ->
            ?LLOG(debug, "RESULT: ~p", [Result]);
        _ ->
            ok
    end,
    case Result of
        ok ->
            ok;
        {ok, QueryResult} ->
            {ok, QueryResult};
        {error, {cql_error, Code, Msg}} ->
            {error, {cql_error, Code, Msg}};
        {error, Error} when Tries > 1 ->
            ?LLOG(error, "error in query ~s: ~p, retrying", [Query, Error]),
            timer:sleep(1000),
            % Next time forget the pid and get a new one
            query(SrvId, Query, Values, Level, Tries-1);
        {error, Error} ->
            {error, Error}
    end;

query(SrvId, Query, Values, Level, Tries) when is_atom(SrvId), Tries > 0 ->
    case nkpacket_pool:get_conn_pid(SrvId) of
        {ok, Pid, _} ->
            query({SrvId, Pid}, Query, Values, Level, Tries);
        {error, Error} when Tries > 1 ->
            ?LLOG(notice, "error in get_connection: ~p, retrying", [Error]),
            timer:sleep(1000),
            query(SrvId, Query, Values, Level, Tries-1);
        {error, Error} ->
            {error, Error}
    end;

query(_SrvId, _Query, _Values, _Level, _Tries) ->
    {error, too_many_tries}.


