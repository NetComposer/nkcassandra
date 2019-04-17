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

-module(
nkcassandra_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([plugin_deps/0, plugin_config/3, plugin_cache/3,
         plugin_start/3, plugin_update/4]).
-export([conn_resolve/3, conn_start/1, conn_stop/1]).

-include("nkcassandra.hrl").
-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkpacket/include/nkpacket.hrl").

-define(LLOG(Type, Txt, Args),lager:Type("NkCASSANDRA "++Txt, Args)).


%% ===================================================================
%% Plugin callbacks
%% ===================================================================


%% @doc
plugin_deps() ->
    [].

%% @doc
%% As url can use:
%% "cassandra://[user:pass@]host[:port][;transport=tcp|tls]
%% "tcp|tls://[user:pass@]host[:port]
plugin_config(_SrvId, Config, #{class:=?PACKAGE_CLASS_CASSANDRA}) ->
    Syntax = #{
        targets => {list, #{
            url => binary,                  % Can use tcp, tls, user, password
            weight => {integer, 1, 1000},
            pool => {integer, 1, none},
            '__mandatory' => [url]
        }},
        keyspace => binary,
        debug => boolean,
        resolve_interval => {integer, 0, none},
        '__mandatory' => [targets]
    },
    nkserver_util:parse_config(Config, Syntax).


plugin_cache(_SrvId, Config, _Service) ->
    Cache = #{
        keyspace => maps:get(keyspace, Config, undefined),
        debug => maps:get(debug, Config, false)
    },
    {ok, Cache}.


%% @doc
plugin_start(SrvId, Config, Service) ->
    insert(SrvId, Config, Service).


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

%% @private
insert(SrvId, Config, Service) ->
    Targets = maps:get(targets, Config, []),
    PoolConfig = Config#{
        targets => Targets,
        debug => maps:get(debug, Config, false),
        resolve_interval => maps:get(resolve_interval, Config, 0),
        conn_resolve_fun => fun ?MODULE:conn_resolve/3,
        conn_start_fun => fun ?MODULE:conn_start/1,
        conn_stop_fun => fun ?MODULE:conn_stop/1,
        keyspace => maps:get(keyspace, Config, undefined),
        srv => SrvId
    },
    Spec = #{
        id => SrvId,
        start => {nkpacket_pool, start_link, [SrvId, PoolConfig]}
    },
    case nkserver_workers_sup:update_child2(SrvId, Spec, #{}) of
        {ok, Op, _Pid} ->
            ?SRV_LOG(info, "pooler updated (~p)", [Op], Service),
            ok;
        {error, Error} ->
            ?SRV_LOG(notice, "pooler start/update error: ~p", [Error], Service),
            {error, Error}
    end.


%% @private
conn_resolve(#{url:=Url}, Config, _Pid) ->
    case nkpacket_resolve:resolve(Url, #{protocol=>nkcassandra_protocol}) of
        {ok, List1} ->
            do_conn_resolve(List1, Config, []);
        {error, Error} ->
            {error, Error}
    end.

%% @private
do_conn_resolve([], _UserOpts, Acc) ->
    {ok, lists:reverse(Acc), #{}};

do_conn_resolve([Conn|Rest], UserOpts, Acc) ->
    #nkconn{protocol=nkcassandra_protocol, opts=Opts} = Conn,
    Opts2 = maps:merge(Opts, UserOpts),
    do_conn_resolve(Rest, UserOpts, [Conn#nkconn{opts=Opts2}|Acc]).


%% @private
%%%% What we really do is start and monitor the CQERL supervisor
%%conn_start(#nkconn{transp=_Transp, ip=Ip, port=Port, opts=Opts}) ->
%%    %Opts1 = [{nodes, [{binary_to_list(nklib_util:to_host(Ip)), Port}]}],
%%    Opts1 = [{nodes, [{Ip, Port}]}],
%%    Opts2 = case Opts of
%%        #{keyspace:=KeySpace} ->
%%            [{keyspace, KeySpace}|Opts1];
%%        _ ->
%%            Opts
%%    end,
%%    ecql:connect(Opts2).


%% What we really do is start and monitor the CQERL supervisor
conn_start(#nkconn{transp=_Transp, ip=Ip, port=Port, opts=Opts}) ->
    Opts2 = maps:with([srv, debug, keyspace, user, password], Opts),
    nkcassandra_protocol:connect(Ip, Port, Opts2).


conn_stop(Pid) ->
    nkcassandra_protocol:stop(Pid).








