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

%% @doc 
-module(nkcassandra_sample).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("nkservice/include/nkservice.hrl").
-define(SRV, cassandra_sample).

%% ===================================================================
%% Public
%% ===================================================================



%% @doc Starts the service
start() ->
    Spec = #{
        plugins => [?MODULE],
        packages => [
            #{
                id => cass1,
                class => 'Cassandra',
                config => #{
                    targets => [
                        #{
                            url => "tcp://127.0.0.1"
                        }
                    ],
                    %keyspace => <<"key1">>,
                    debug => true
                }
            }
        ],
        modules => [
            #{
                id => s1,
                class => luerl,
                code => s1(),
                debug => true

            }
        ]
    },
    nkservice:start(?SRV, Spec).


%% @doc Stops the service
stop() ->
    nkservice:stop(?SRV).


% select * from system.eventlog
luerl_query(Sql) ->
    nkservice_luerl_instance:call({?SRV, s1, main}, [query], [nklib_util:to_binary(Sql)], 30000).


s1() -> <<"
    cassConfig = {
        targets = {
            {
                url = 'tcp://127.0.0.1'
            }
        },
        resolveInterval = 0,
        debug = true
    }

    cass = startPackage('Cassandra', cassConfig)

    function query(sql)
        return cass.query(sql)
    end

">>.


query1(C) ->
    nkcassandra:query(C, "SELECT * FROM users;").


query2(C) ->
    Ref = nkcassandra:query_async(C, "SELECT * FROM users;"),
    nkcassandra:query_async_wait(Ref, 1000).

