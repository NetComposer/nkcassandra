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

%% @doc NkCASSANDRA service

-module(nkcassandra_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([plugin_deps/0, plugin_config/3,
         plugin_start/3, plugin_update/4]).
-export([conn_resolve/3, conn_start/1, conn_stop/1]).

-include("nkcassandra.hrl").
-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkpacket/include/nkpacket.hrl").
-include_lib("cqerl/include/cqerl.hrl").

-define(LLOG(Type, Txt, Args),lager:Type("NkCASSANDRA "++Txt, Args)).


%% ===================================================================
%% Plugin callbacks
%% ===================================================================


%% @doc
plugin_deps() ->
    [].

%% @doc
plugin_config(_SrvId, Config, #{class:=?PACKAGE_CASSANDRA}) ->
    Syntax = #{
        targets => {list, #{
            url => binary,
            weight => {integer, 1, 1000},
            '__mandatory' => [url]
        }},
        keyspace => binary,
        debug => boolean,
        resolve_interval => {integer, 0, none},
        '__mandatory' => [targets]
    },
    nkserver_util:parse_config(Config, Syntax).


%% @doc
plugin_start(SrvId, Config, Service) ->
    insert(SrvId, Config, Service).



%% @doc
%% @doc
plugin_update(SrvId, NewConfig, OldConfig, Service) ->
    case NewConfig of
        OldConfig ->
            ok;
        _ ->
            insert(SrvId, NewConfig, Service)
    end.



%% ===================================================================
%% Internal
%% ===================================================================

%% We start a pooler to a number of cassandra instances, with different weight
%% When a new request arrives, we start a cqerl pool for each url and
%% monitor the cqerl supervisor
%% When we call nkcassandra:get_client/2, we first check if the supervisor
%% is available (and start it if it is not), then we use the standard cqerl client
%% pooling system

%% @private
insert(SrvId, Config, Service) ->
    Targets1 = maps:get(targets, Config, []),
    Targets2 = [T#{pool=>1} || T <- Targets1],
    PoolConfig = Config#{
        targets => Targets2,
        debug => maps:get(debug, Config, false),
        resolve_interval => maps:get(resolve_interval, Config, 0),
        conn_resolve_fun => fun ?MODULE:conn_resolve/3,
        conn_start_fun => fun ?MODULE:conn_start/1,
        conn_stop_fun => fun ?MODULE:conn_stop/1
    },
    Spec = #{
        id => SrvId,
        start => {nkpacket_pool, start_link, [SrvId, PoolConfig]}
    },
    case nkserver_workers_sup:update_child(SrvId, Spec, #{}) of
        {added, _} ->
            ?SRV_LOG(info, "pooler started", [], Service),
            ok;
        upgraded ->
            ?SRV_LOG(info, "pooler upgraded", [], Service),
            ok;
        not_updated ->
            ?SRV_LOG(debug, "pooler didn't upgrade", [], Service),
            ok;
        {error, Error} ->
            ?SRV_LOG(notice, "pooler start/update error: ~p", [Error], Service),
            {error, Error}
    end.


%% @private
conn_resolve(#{url:=Url}, Config, _Pid) ->
    ResOpts = #{schemes=>#{cassandra=>cassandra, tcp=>cassandra}},
    UserOpts = maps:with([keyspace], Config),
    case nkpacket_resolve:resolve(Url, ResOpts) of
        {ok, List1} ->
            do_conn_resolve(List1, UserOpts, []);
        {error, Error} ->
            {error, Error}
    end.

%% @private
do_conn_resolve([], _UserOpts, Acc) ->
    {ok, lists:reverse(Acc)};

do_conn_resolve([Conn|Rest], UserOpts, Acc) ->
    case Conn of
        #nkconn{protocol=cassandra, transp=Transp, port=Port, opts=Opts} ->
            Transp2 = case Transp of
                tcp ->
                    tcp;
                undefined ->
                    tcp
            end,
            Port2 = case Port of
                0 ->
                    9042;
                _ ->
                    Port
            end,
            Opts2 = maps:merge(Opts, UserOpts),
            Conn2 = Conn#nkconn{transp=Transp2, port=Port2, opts=Opts2},
            do_conn_resolve(Rest, UserOpts, [Conn2|Acc]);
        O ->
            {error, {invalid_protocol, O}}
    end.


%% @private
%% What we really do is start and monitor the CQERL supervisor
conn_start(#nkconn{transp=_Transp, ip=Ip, port=Port, opts=Opts}=Conn) ->
    % Opts can include keyspace, auth (but fails with keyspace!)
    Opts2 = maps:to_list(maps:with([auth], Opts)),
    % See cqerl:get_client/2
    Key = cqerl_client:make_key({Ip, Port}, Opts2),
    case  ets:lookup(cqerl_client_tables, Key) of
        [{client_table, Key, SupPid, _Table}] ->
            {ok, SupPid};
        [] ->
            case gen_server:call(cqerl, {start_clients, {Ip, Port}, Opts2}, infinity) of
                ok ->
                    conn_start(Conn);
                {error, Error} ->
                    {error, Error}
            end
    end.


conn_stop(Pid) ->
    % Stop the CQERL supervisor
    sys:terminate(Pid, normal),
    ok.








