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

%% @doc 
-module(nkcassandra_sample).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("nkserver/include/nkserver.hrl").

-define(SRV, sample).



%% ===================================================================
%% Public
%% ===================================================================


start() ->
    Config = #{
        targets => [#{url => "tcp://192.168.0.111", pool=>100}],
        debug => false
    },
    {ok, _} = nkserver:start_link(<<"Cassandra">>, ?SRV, Config),
    ok.


bench_prepare() ->
    create_keyspace(),
    create_table(),
    truncate().



%% @doc
create_keyspace() ->
    Query = <<
        "CREATE KEYSPACE nkcass_bench "
        "WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};"
    >>,
    case nkcassandra:query(?SRV, Query) of
        {error, {cql_error, 9216, _}} ->
            % Already created
            ok;
        {ok, {<<"CREATED">>,<<"KEYSPACE">>,<<"nkcass_bench">>}} ->
            ok
    end.


create_table() ->
    Query = <<"CREATE TABLE nkcass_bench.table1 (id text PRIMARY KEY, name text);">>,
    case nkcassandra:query(?SRV, Query) of
        {error, {cql_error, 9216, _}} ->
            % Already created
            ok;
        {ok, {<<"CREATED">>,<<"TABLE">>,<<"nkcass_bench">>}} ->
            ok
    end.

truncate() ->
    Query = <<"TRUNCATE nkcass_bench.table1;">>,
    ok = nkcassandra:query(?SRV, Query).



bench(Proc, Total) ->
    bench_prepare(),
    lager:error("NKLOG Q3"),
    Bin = binary:copy(<<"a">>, 400),
    Each = Total div Proc,
    Self = self(),
    Start = nklib_date:epoch(msecs),
    lists:foreach(
        fun(P) ->
            P2 = nklib_util:lpad(P, 8, $0),
            {ok, ConnPid, _} = nkpacket_pool:get_conn_pid(?SRV),
            {ok, _} = nkcassandra_protocol:prepare(ConnPid, test_insert,
                <<"INSERT INTO nkcass_bench.table1 (id, name) VALUES (?,?)">>),
            spawn_link(fun() -> bench2(P2, Bin, Each, Self, ConnPid) end)
        end,
        lists:seq(1, Proc)),
    lists:foreach(
        fun(P) ->
            P2 = nklib_util:lpad(P, 8, $0),
            receive P2 ->
                ok
            after
                100000 ->
                    error(P2)
            end
        end,
        lists:seq(1, Proc)),
    Stop = nklib_date:epoch(msecs),
    Total / (Stop - Start) * 1000.



bench2(Hd, Bin, N, Pid, ConnPid) when N > 0 ->
    N2 = nklib_util:lpad(N, 8, $0),
    Id = <<Hd/binary, N2/binary>>,

    % Normal option:
    Q = <<"
        INSERT INTO nkcass_bench.table1 (id, name)
        VALUES ('", Id/binary, "', '", Bin/binary, "');
    ">>,
    ok = nkcassandra:query({?SRV, ConnPid}, Q),

%%    % Faster
%%    ok = nkcassandra_protocol:execute(ConnPid, test_insert, [Id, N2]),

    bench2(Hd, Bin, N-1, Pid, ConnPid);




bench2(Hd, _Bin, _N, Pid, _ConnPid) ->
    %lager:error("NKLOG SEND ~p", [Hd]),
    Pid ! Hd,
    %nkcassandra_protocol:stop(_ConnPid),
    ok.



%%bench(Proc, Total) ->
%%    Bin = binary:copy(<<"a">>, 400),
%%    Each = Total div Proc,
%%    Self = self(),
%%    Start = nklib_date:epoch(msecs),
%%    lists:foreach(
%%        fun(P) ->
%%            {ok, C} = nkcassandra_protocol:connect({192,168,0,111}, 9042, #{}),
%%            P2 = nklib_util:lpad(P, 8, $0),
%%            spawn(fun() -> bench2(C, P2, Bin, Each, Self) end)
%%        end,
%%        lists:seq(1, Proc)),
%%    lists:foreach(
%%        fun(P) ->
%%            P2 = nklib_util:lpad(P, 8, $0),
%%            receive P2 ->
%%                ok
%%            after
%%                100000 ->
%%                    error(P2)
%%            end
%%        end,
%%        lists:seq(1, Proc)),
%%    Stop = nklib_date:epoch(msecs),
%%    Total / (Stop - Start) * 1000.
%%
%%
%%
%%bench2(C, Hd, Bin, N, Pid) when N > 0 ->
%%    N2 = nklib_util:lpad(N, 8, $0),
%%    Q = <<"
%%        INSERT INTO nkcass_bench.table2 (id, name)
%%        VALUES ('", Hd/binary, N2/binary, "', '", Bin/binary, "');
%%    ">>,
%%    ok = nkcassandra_protocol:query(C, Q),
%%    bench2(C, Hd, Bin, N-1, Pid);
%%
%%bench2(_C, Hd, _Bin, _N, Pid) ->
%%    %lager:error("NKLOG SEND ~p", [Hd]),
%%    Pid ! Hd,
%%    ok.

