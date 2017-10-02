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

%% @doc NkCASSANDRA callbacks

-module(nkcassandra_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([plugin_deps/0, plugin_syntax/0, plugin_config/2, plugin_start/2]).

-include("nkcassandra.hrl").
-include_lib("nkservice/include/nkservice.hrl").
-include_lib("brod/include/brod.hrl").



%% ===================================================================
%% Types
%% ===================================================================

% -type continue() :: continue | {continue, list()}.




%% ===================================================================
%% Offered callbacks
%% ===================================================================


%% ===================================================================
%% Plugin callbacks
%%
%% These are used when NkCASSANDRA is started as a NkSERVICE plugin
%% ===================================================================


plugin_deps() ->
    [].


plugin_syntax() ->
	#{
	    nkcassandra =>
            {list, #{
                cluster => binary,
                nodes => {list, #{
                    host => host,
                    port => {integer, 1, 65535},
                    '__defaults' => #{host=><<"127.0.0.1">>, port=>9042}
                }},
                keyspace => binary,
                '__mandatory' => [cluster, nodes]
           }}
}.


plugin_config(#{nkcassandra:=List}=Config, #{id:=SrvId}) ->
    case parse_clients(SrvId, List, #{}) of
        {ok, Clusters} ->
            {ok, Config#{nkcassandra_clusters=>Clusters}};
        {error, Error} ->
            {error, Error}
    end;

plugin_config(Config, _Service) ->
    {ok, Config}.


plugin_start(#{nkcassandra_clusters:=Clients}=Config, _Service) ->
    case start_clusters(maps:to_list(Clients)) of
        ok ->
            {ok, Config};
        {error, Error} ->
            {error, Error}
    end;

plugin_start(Config, _Service) ->
    {ok, Config}.



%% ===================================================================
%% Util
%% ===================================================================

%% @private
parse_clients(_SrvId, [], Acc) ->
    {ok, Acc};

parse_clients(SrvId, [#{cluster:=Cluster, nodes:=Nodes}=Map|Rest], Acc) ->
    Cluster2 = binary_to_atom(Cluster, utf8),
    case maps:is_key(Cluster, Acc) of
        false ->
            Nodes2 = [{Host, Port} || #{host:=Host, port:=Port} <- Nodes],
            Opts = case Map of
                #{keyspace:=KeySpace} ->
                    [{keyspace, binary_to_atom(KeySpace, utf8)}];
                _ ->
                    []
            end,
            parse_clients(SrvId, Rest, Acc#{Cluster2=>{Nodes2, Opts}});
        true ->
            {error, duplicated_cluster}
    end.



%% @private
start_clusters([]) ->
    ok;

start_clusters([{Cluster, {Hosts, Opts}}|Rest]) ->
    lager:notice("cqerl_cluster:add_nodes(~p,~p,~p)", [Cluster, Hosts, Opts]),
    case cqerl_cluster:add_nodes(Cluster, Hosts, Opts) of
        ok ->
            lager:notice("NkCassandra started cluster '~s' (~p, ~p)", [Cluster, Hosts, Opts]),
            start_clusters(Rest);
        Other ->
            {error, {could_not_start_cqerl_cluster, Other}}
    end.


